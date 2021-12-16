/*
 *  The contents of this file are subject to the Initial
 *  Developer's Public License Version 1.0 (the "License");
 *  you may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *  http://www.ibphoenix.com/main.nfs?a=ibphoenix&page=ibp_idpl.
 *
 *  Software distributed under the License is distributed AS IS,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied.
 *  See the License for the specific language governing rights
 *  and limitations under the License.
 *
 *  The Original Code was created by Vladyslav Khorsun
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2012 Vladyslav Khorsun <hvlad@users.sourceforge.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

#ifndef JRD_DPM_PREFETCH_H
#define JRD_DPM_PREFETCH_H

#include "../jrd/jrd.h"
#include "../jrd/ods.h"
#include "../jrd/Relation.h"
#include "../jrd/cch_proto.h"


namespace Jrd {

struct DPMPrefetchInfo_1
{
	enum KIND {FULLSCAN, RECS_BITMAP, PAGES_BITMAP};

	void reset(bool sweeper)
	{
		reset(FULLSCAN, sweeper);
	}

	void reset(PageBitmap* bmPages, bool sweeper)
	{
		reset(PAGES_BITMAP, sweeper);
		m_pages = bmPages;
	}

	void reset(RecordBitmap* bmRecs, bool sweeper)
	{
		reset(RECS_BITMAP, sweeper);
		m_recs = bmRecs;
	}

	void nextDP(Jrd::thread_db* tdbb, const RelationPages* relPages,
		const Ods::pointer_page* ppage, int slot);

private:
	void reset(KIND kind, bool sweeper)
	{
		m_kind = kind;
		m_seqDone = m_seqCheck = 0;
		m_slotDone = m_slotCheck = 0;
		m_len = 0;
		m_sweeper = sweeper;
		m_pages = NULL;
	}

	KIND m_kind;
	ULONG m_seqCheck;		// PP and slot when make new request
	int	m_slotCheck;	// 
	int m_seqDone;		// max PP and DP slot from last prefetch request
	int	m_slotDone;		// 
	int	m_len;			// number of pages in last prefetch request
	bool m_sweeper;

	union
	{
		PageBitmap* m_pages;
		RecordBitmap* m_recs;
	};
};

struct DPMPrefetchInfo
{
	enum KIND {FULLSCAN, RECS_BITMAP, PAGES_BITMAP};

	void reset(bool enabled)
	{
		m_pages = nullptr;
		reset(FULLSCAN, enabled);
	}

	void reset(PageBitmap* bmPages, bool enabled)
	{
		m_pages = bmPages;
		reset(PAGES_BITMAP, enabled && m_pages);
	}

	void reset(RecordBitmap* bmRecs, bool enabled)
	{
		m_recs = bmRecs;
		reset(RECS_BITMAP, enabled && m_recs);
	}

	void nextDP(Jrd::thread_db* tdbb, const RelationPages* relPages,
		const Ods::pointer_page* ppage, int slot);

private:
	class PointerPageHolder
	{
	public:
		PointerPageHolder(Jrd::thread_db* tdbb, const Jrd::RelationPages* relPages, 
						  const Ods::pointer_page* ppage) :
			m_tdbb(tdbb),
			m_relPages(relPages),
			m_ppage(ppage),
			m_window(relPages->rel_pg_space_id, 0)
		{}

		~PointerPageHolder()
		{
			release();
		}

		const Ods::pointer_page* fetch(ULONG ppSequence);

		ULONG getPageNum() const
		{
			return m_window.win_page.getPageNum();
		}

	private:
		void release()
		{
			if (m_window.win_bdb != nullptr)
				CCH_RELEASE(m_tdbb, &m_window);
		}

		Jrd::thread_db* const m_tdbb;
		const Jrd::RelationPages* const m_relPages;
		const Ods::pointer_page* const m_ppage;
		Jrd::win m_window;
	};

	void reset(KIND kind, bool enabled)
	{
		m_kind = kind;
		m_enabled = enabled;
		m_maxLen = MAX_PAGES_PER_PIOREQ;
		m_trigPage = 0;
		m_checkPage = 0;
		m_lastPage = (ULONG) -1;
		m_len = 0;
		m_distance = 0;
	}

	// Makes prefetch request and update all three control page numbers
	void makePrefetch(Jrd::thread_db* tdbb, const RelationPages* relPages,
		const Ods::pointer_page* ppage);

	KIND m_kind;
	bool m_enabled;
	ULONG m_maxLen;

	// Logical page numbers numbers below
	ULONG m_trigPage;		// when make new prefetch
	ULONG m_checkPage;		// when check state of last prefetch
	ULONG m_lastPage;		// last page in last prefetch 

	ULONG m_len;			// number of pages in last prefetch 
	ULONG m_distance;		// distance between trigger and last pages, 0 .. m_len-1

	union
	{
		PageBitmap* m_pages;
		RecordBitmap* m_recs;
	};
};

} // namespace Jrd

#endif //  JRD_DPM_PREFETCH_H
