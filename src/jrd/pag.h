/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		pag.h
 *	DESCRIPTION:	Page interface definitions
 *
 * The contents of this file are subject to the Interbase Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy
 * of the License at http://www.Inprise.com/IPL.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code was created by Inprise Corporation
 * and its predecessors. Portions created by Inprise Corporation are
 * Copyright (C) Inprise Corporation.
 *
 * All Rights Reserved.
 * Contributor(s): ______________________________________.
 */

/*
 * Modified by: Patrick J. P. Griffin
 * Date: 11/29/2000
 * Problem:   Bug 116733 Too many generators corrupt database.
 *            DPM_gen_id was not calculating page and offset correctly.
 * Change:    Add pgc_gpg, number of generators per page,
 *            for use in DPM_gen_id.
 */


#ifndef JRD_PAG_H
#define JRD_PAG_H

#include "../include/fb_blk.h"
#include "../common/classes/array.h"
#include "../common/classes/locks.h"
#include "../common/classes/fb_string.h"
#include "../jrd/ods.h"
#include "../jrd/lls.h"
#include "../jrd/os/pio.h"

namespace Jrd {

// Page control block -- used by PAG to keep track of critical constants
/**
class PageControl : public pool_alloc<type_pgc>
{
    public:
	SLONG pgc_high_water;		// Lowest PIP with space
	SLONG pgc_ppp;				// Pages per pip
	SLONG pgc_pip;				// First pointer page
	ULONG pgc_bytes;			// Number of bytes of bit in PIP
	ULONG pgc_tpt;				// Transactions per TIP
	ULONG pgc_gpg;				// Generators per generator page
};
**/

// page spaces below TRANS_PAGE_SPACE contain regular database pages
// TEMP_PAGE_SPACE and page spaces above TEMP_PAGE_SPACE contain temporary pages
// TRANS_PAGE_SPACE is pseudo space to store transaction numbers in precedence stack
// INVALID_PAGE_SPACE is to ???
const USHORT INVALID_PAGE_SPACE	= 0;
const USHORT DB_PAGE_SPACE		= 1;
const USHORT TRANS_PAGE_SPACE	= 255;
const USHORT TEMP_PAGE_SPACE	= 256;

const USHORT PAGES_IN_EXTENT	= 8;

class jrd_file;
class Database;
class thread_db;
class PageSpace;
class PageManager;
//class PIORequest;

// maximum pages to prefetch at once
const int MAX_PREFETCH_PAGES = 16;

// number of concurrent PIO requests when database is 
const int MAX_PIOREQ_PER_PAGESPACE_SS = 64;		// in shared mode
const int MAX_PIOREQ_PER_PAGESPACE_CS = 8;		// not in shared mode

// Top level code (dpm, btr, etc) create prefetch requests and put it into common que:
// prf = pageSpace->getPrefetchReq();
// prf->m_pageNumbers.push(page_num); 
// ...
// prf->m_pageNumbers.push(page_num); 
// pageMgr->addPrefetchReq(prf);

extern "C" {
static int cmpULong(const void* a, const void* b)
{
	const ULONG* ulA = (ULONG*) a;
	const ULONG* ulB = (ULONG*) b;

	if (*ulA > *ulB)
		return 1;

	if (*ulA < *ulB)
		return -1;

	return 0;
}
} // extern C


typedef Firebird::HalfStaticArray<ULONG, MAX_PREFETCH_PAGES> PrefetchArray;

class PrefetchReq : public PrefetchArray
{
typedef PrefetchArray inherited;

public:
	PrefetchReq(Firebird::MemoryPool& pool, const size_type InitialCapacity, PageSpace* pageSpace) :
		inherited(pool, InitialCapacity),
		m_pageSpace(pageSpace),
		m_sorted(false),
		m_next(NULL)
	{
	}

	void sort()
	{
		if (!m_sorted)
		{
			qsort(begin(), getCount(), sizeof(ULONG), cmpULong);
			m_sorted = true;
		}
	}

	void clear()
	{
		inherited::clear();
		m_sorted = false;
	}

	PageSpace* getPageSpace() const
	{
		return m_pageSpace;
	}

	Firebird::string dump() const;

private:
	PageSpace*	m_pageSpace;
	bool		m_sorted;
	PrefetchReq*	m_next;

	friend class PageSpace;
};


class PageSpace : public pool_alloc<type_PageSpace>
{
public:
	explicit PageSpace(Database* aDbb, USHORT aPageSpaceID);
	~PageSpace();

	USHORT pageSpaceID;
	Firebird::AtomicCounter pipHighWater;		// Lowest PIP with space
	Firebird::AtomicCounter pipWithExtent;		// Lowest PIP with free extent
	ULONG pipFirst;								// First pointer page
	ULONG scnFirst;								// First SCN's page

	jrd_file*	file;

	static inline bool isTemporary(USHORT aPageSpaceID)
	{
		return (aPageSpaceID >= TEMP_PAGE_SPACE);
	}

	inline bool isTemporary() const
	{
		return isTemporary(pageSpaceID);
	}

	static inline SLONG generate(const PageSpace* Item)
	{
		return Item->pageSpaceID;
	}

	// how many pages allocated
	ULONG actAlloc();
	static ULONG actAlloc(const Database* dbb);

	// number of last allocated page
	ULONG maxAlloc();
	static ULONG maxAlloc(const Database* dbb);

	// number of last used page
	ULONG lastUsedPage();
	static ULONG lastUsedPage(const Database* dbb);

	// number of used pages
	ULONG usedPages();
	static ULONG usedPages(const Database* dbb);

	// extend page space
	bool extend(thread_db*, const ULONG, const bool);

	// get SCN's page number
	ULONG getSCNPageNum(ULONG sequence);
	static ULONG getSCNPageNum(const Database* dbb, ULONG sequence);

	bool prefetchEnabled() const;
	void registerPrefetch(const PrefetchArray& prf);

	PrefetchReq* newPrefetchReq();
	void freePrefetchReq(PrefetchReq* prf);

	PIORequest* newPIORequest();
	void freePIORequest(PIORequest* pio);

private:
	ULONG	maxPageNumber;
	Database* dbb;
	Firebird::Array<PrefetchReq*> allPrefetch;
	Firebird::AtomicPointer<PrefetchReq> freePrefetch;
	Firebird::Array<PIORequest*> allPIOReqs;
	Firebird::AtomicPointer<PIORequest> freePIOReqs;
};

class PageManager : public pool_alloc<type_PageManager>
{
public:
	explicit PageManager(Database* aDbb, Firebird::MemoryPool& aPool) :
		dbb(aDbb),
		pageSpaces(aPool),
		pool(aPool),
		prefetchQue(aPool)
	{
		pagesPerPIP = 0;
		bytesBitPIP = 0;
		transPerTIP = 0;
		gensPerPage = 0;
		pagesPerSCN = 0;
		tempPageSpaceID = 0;
		tempFileCreated = false;

		addPageSpace(DB_PAGE_SPACE);
	}

	~PageManager()
	{
		while (pageSpaces.hasData())
			delete pageSpaces.pop();
	}

	PageSpace* findPageSpace(const USHORT pageSpaceID) const;

	void initTempPageSpace(thread_db* tdbb);
	USHORT getTempPageSpaceID(thread_db* tdbb);

	void closeAll();

	void addPrefetchReq(PrefetchReq* prf, bool ahead = false);
	PrefetchReq* getPrefetchReq();
	PrefetchReq* nextPrefetchReq(PageSpace* pageSpace);

	ULONG pagesPerPIP;			// Pages per pip
	ULONG bytesBitPIP;			// Number of bytes of bit in PIP
	ULONG transPerTIP;			// Transactions per TIP
	ULONG gensPerPage;			// Generators per generator page
	ULONG pagesPerSCN;			// Slots per SCN's page

	PIOPort pioPort;

private:
	typedef Firebird::SortedArray<PageSpace*, Firebird::EmptyStorage<PageSpace*>,
		USHORT, PageSpace> PageSpaceArray;

	PageSpace* addPageSpace(const USHORT pageSpaceID);
	void delPageSpace(const USHORT pageSpaceID);

	Database* dbb;
	PageSpaceArray pageSpaces;
	Firebird::MemoryPool& pool;
	Firebird::Mutex	initTmpMtx;
	USHORT tempPageSpaceID;
	bool tempFileCreated;

	Firebird::SyncObject syncReqQue;
	Firebird::Array<PrefetchReq*> prefetchQue;
};

class PageNumber
{
public:
	// CVC: To be completely in sync, the second param would have to be TraNumber
	inline PageNumber(const USHORT aPageSpace, const ULONG aPageNum)
		: pageNum(aPageNum), pageSpaceID(aPageSpace)
	{
		// Some asserts are commented cause 0 was also used as 'does not matter' pagespace
		// fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
	}

	// Required to be able to keep it in Firebird::Stack
	inline PageNumber()
		: pageNum(0), pageSpaceID(INVALID_PAGE_SPACE)
	{ }

	inline PageNumber(const PageNumber& from)
		: pageNum(from.pageNum), pageSpaceID(from.pageSpaceID)
	{ }

	inline ULONG getPageNum() const
	{
		// fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		return pageNum;
	}

	inline USHORT getPageSpaceID() const
	{
		fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		return pageSpaceID;
	}

	inline USHORT setPageSpaceID(const USHORT aPageSpaceID)
	{
		fb_assert(aPageSpaceID != INVALID_PAGE_SPACE);
		pageSpaceID = aPageSpaceID;
		return pageSpaceID;
	}

	inline bool isTemporary() const
	{
		fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		return PageSpace::isTemporary(pageSpaceID);
	}

	inline static USHORT getLockLen()
	{
		return 2 * sizeof(ULONG);
	}

	inline void getLockStr(UCHAR* str) const
	{
		fb_assert(pageSpaceID != INVALID_PAGE_SPACE);

		memcpy(str, &pageNum, sizeof(ULONG));
		str += sizeof(ULONG);

		const ULONG val = pageSpaceID;
		memcpy(str, &val, sizeof(ULONG));
	}

	inline PageNumber& operator=(const PageNumber& from)
	{
		pageSpaceID = from.pageSpaceID;
		// fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		pageNum	= from.pageNum;
		return *this;
	}

	inline ULONG operator=(const ULONG from)
	{
		// fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		pageNum	= from;
		return pageNum;
	}

	inline bool operator==(const PageNumber& other) const
	{
		return (pageNum == other.pageNum) && (pageSpaceID == other.pageSpaceID);
	}

	inline bool operator!=(const PageNumber& other) const
	{
		return !(*this == other);
	}

	inline bool operator>(const PageNumber& other) const
	{
		fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		fb_assert(other.pageSpaceID != INVALID_PAGE_SPACE);
		return (pageSpaceID > other.pageSpaceID) ||
			((pageSpaceID == other.pageSpaceID) && (pageNum > other.pageNum));
	}

	inline bool operator>=(const PageNumber& other) const
	{
		fb_assert(pageSpaceID != INVALID_PAGE_SPACE);
		fb_assert(other.pageSpaceID != INVALID_PAGE_SPACE);
		return (pageSpaceID > other.pageSpaceID) ||
			((pageSpaceID == other.pageSpaceID) && (pageNum >= other.pageNum));
	}

	inline bool operator<(const PageNumber& other) const
	{
		return !(*this >= other);
	}

	inline bool operator<=(const PageNumber& other) const
	{
		return !(*this > other);
	}

	/*
	inline operator ULONG() const
	{
		return pageNum;
	}
	*/

private:
	ULONG	pageNum;
	USHORT	pageSpaceID;
};

const PageNumber ZERO_PAGE_NUMBER(DB_PAGE_SPACE, 0);
const PageNumber HEADER_PAGE_NUMBER(DB_PAGE_SPACE, HEADER_PAGE);

typedef Firebird::Stack<PageNumber> PageStack;

} //namespace Jrd

#endif // JRD_PAG_H
