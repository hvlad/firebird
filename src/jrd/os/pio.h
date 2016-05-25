/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		pio.h
 *	DESCRIPTION:	File system interface definitions
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
 *
 * 2002.10.29 Sean Leyne - Removed obsolete "Netware" port
 *
 * 2002.10.30 Sean Leyne - Removed support for obsolete "PC_PLATFORM" define
 *
 */

#ifndef JRD_PIO_H
#define JRD_PIO_H

#include "../include/fb_blk.h"
#include "../common/classes/rwlock.h"
#include "../common/classes/array.h"
#include "../common/classes/File.h"
#include "../common/classes/fb_string.h"
#include "../common/classes/SyncObject.h"

#ifdef UNIX

#define USE_LIBAIO

#if defined (USE_LIBAIO)
#include <libaio.h>
#include <sys/eventfd.h>
#include <poll.h>
#elif defined (HAVE_AIO_H)
#include <aio.h>
#endif

#endif // UNIX

namespace Jrd {


#ifdef WIN_NT
typedef ULONG_PTR PIO_EVENT_T;
#else
typedef void* PIO_EVENT_T;
#endif

const PIO_EVENT_T PIO_EVENT_WAKEUP	= (PIO_EVENT_T) 0x1;
const PIO_EVENT_T PIO_EVENT_TIMEOUT	= (PIO_EVENT_T) 0x2;
const PIO_EVENT_T PIO_EVENT_IO		= (PIO_EVENT_T) 0x3;


#ifdef UNIX

class jrd_file : public pool_alloc_rpt<SCHAR, type_fil>
{
public:
	jrd_file*	fil_next;		// Next file in database
	ULONG fil_min_page;			// Minimum page number in file
	ULONG fil_max_page;			// Maximum page number in file
	USHORT fil_sequence;		// Sequence number of file
	USHORT fil_fudge;			// Fudge factor for page relocation
	int fil_desc;
	//int *fil_trace;			// Trace file, if any
	Firebird::Mutex fil_mutex;
	USHORT fil_flags;
	SCHAR fil_string[1];		// Expanded file name
};

#endif


#ifdef WIN_NT

class jrd_file : public pool_alloc_rpt<SCHAR, type_fil>
{
public:

	~jrd_file()
	{
		delete fil_ext_lock;
	}

	jrd_file*	fil_next;				// Next file in database
	ULONG fil_min_page;					// Minimum page number in file
	ULONG fil_max_page;					// Maximum page number in file
	USHORT fil_sequence;				// Sequence number of file
	USHORT fil_fudge;					// Fudge factor for page relocation
	HANDLE fil_desc;					// File descriptor
	Firebird::RWLock* fil_ext_lock;		// file extend lock
	USHORT fil_flags;
	SCHAR fil_string[1];				// Expanded file name
};

#endif


const USHORT FIL_force_write		= 1;
const USHORT FIL_no_fs_cache		= 2;	// not using file system cache
const USHORT FIL_readonly			= 4;	// file opened in readonly mode
const USHORT FIL_sh_write			= 8;	// file opened in shared write mode
const USHORT FIL_no_fast_extend		= 16;	// file not supports fast extending
const USHORT FIL_aio_init			= 32;	// AIO stuff initialized after (re)open

class BufferDesc;
class Database;
class thread_db;

const int MAX_PAGES_PER_PIOREQ = 64;
const int OS_CACHED_IO_SIZE = 64 * 1024;

class BDBPageNum
{
public:
	static const ULONG generate(const BufferDesc* bdb); 
};

typedef	Firebird::SortedArray<
		BufferDesc*, 
		Firebird::EmptyStorage<BufferDesc*>, 
		ULONG, 
		BDBPageNum>	BufferDescArray;


class PIORequest
{
public:
	enum PIOR_STATE {PIOR_CREATED, PIOR_PENDING, PIOR_COMPLETED, PIOR_ERROR};

	PIORequest(Firebird::MemoryPool& pool);

	void addPage(BufferDesc* bdb);
	int  fitPageNumber(ULONG pageNumber, ULONG pageSize) const;

	void setFile(jrd_file* file)
	{
		m_file = file;
	}

	void clear()
	{
		m_file = NULL;
		m_pages.clear();
		m_startIdx = m_count = m_ioSize = 0;
		m_state = PIOR_CREATED;
	}

	bool isEmpty() const
	{
		return m_pages.isEmpty();
	}

	bool isFull() const;
	ULONG minPage() const;
	ULONG maxPage() const;

	void* allocIOBuffer(int sysPageSize);

	// returns buffer used for IO of a few not adjacent pages
	void* getIOBuffer()
	{
		return (m_count > 1) ? m_aligned_buffer : NULL;
	}

	// platrofm-dependent routines

	bool postRead(Database* dbb);
	bool readComplete(thread_db* tdbb);
	void markCompletion(bool error, size_t ioSize);

	int getOSError() const
	{
		return m_osError;
	}

	Firebird::string dump() const;

private:

#ifdef WIN_NT
	typedef OVERLAPPED OSAIO;
#elif defined (USE_LIBAIO)
	typedef struct iocb OSAIO;
#elif defined (HAVE_AIO_H)
	typedef aiocb OSAIO;
#else
#endif
	friend class PageSpace;
	PIORequest*	m_next;

	OSAIO		m_osData;
	jrd_file*	m_file;
	PIOR_STATE	m_state;
	int			m_osError;
	
	Firebird::Array<SCHAR> m_buffer;
	SCHAR*		m_aligned_buffer;

	BufferDescArray	m_pages;

#ifdef WIN_NT
	Firebird::Array<FILE_SEGMENT_ELEMENT> m_segments;
#elif defined USE_LIBAIO
	Firebird::Array<iovec> m_segments;
#endif

	// starting index and number of entries in m_pages array of pending IO operation
	unsigned int m_startIdx;
	unsigned int m_count;
	unsigned int m_ioSize;

public:
	static PIORequest* fromOSData(OSAIO* osData)
	{
		return (PIORequest*) ((SCHAR*) osData - offsetof(PIORequest, m_osData));
	}
};


class PIOPort
{
public:
	PIOPort();
	~PIOPort();

	void addFile(jrd_file* file);
	void removeFile(jrd_file* file);

	PIO_EVENT_T getCompletedRequest(thread_db* tdbb, PIORequest** pio, int timeout);
	void postEvent(PIO_EVENT_T pioEvent);

#if defined (USE_LIBAIO)
	io_context_t getIOContext()
	{ return m_handle; };

	int getEventFD()
	{ return m_eventfd; };
#endif

private:
#ifdef WIN_NT
	HANDLE m_handle;
#elif defined (USE_LIBAIO)
	io_context_t m_handle;
	int m_eventfd;
#elif defined (HAVE_AIO_H)
#else
#endif
	Firebird::SyncObject m_sync;
};


} //namespace Jrd

#endif // JRD_PIO_H

