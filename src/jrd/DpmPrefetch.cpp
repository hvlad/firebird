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

#include "firebird.h"
#include "../jrd/DpmPrefetch.h"

#include "../common/classes/tree.h"
#include "../jrd/cch.h"
#include "../jrd/jrd.h"
#include "../jrd/ods.h"
#include "../jrd/pag.h"
#include "../jrd/Relation.h"
#include "../jrd/sbm.h"

#include "../jrd/cch_proto.h"
#include "../jrd/err_proto.h"
#include "../jrd/pag_proto.h"
#include "../jrd/os/pio_proto.h"

using namespace Jrd;
using namespace Ods;
using namespace Firebird;

namespace Jrd {

void DPMPrefetchInfo_1::nextDP(thread_db* tdbb, const RelationPages* relPages, 
		const pointer_page* ppage, int slot)
{
	if (m_len)
	{
		if (ppage->ppg_sequence < m_seqCheck)
			return;
		if (ppage->ppg_sequence == m_seqCheck && slot < m_slotCheck)
			return;
	}

	if (m_kind == PAGES_BITMAP && !m_pages)
		return;

	if (m_kind == RECS_BITMAP && !m_recs)
		return;

	Database* dbb = tdbb->getDatabase();
	if (!dbb->dbb_bcb->bcb_reader_cnt.value())
		return;

	PageSpace* pageSpace = dbb->dbb_page_manager.findPageSpace(relPages->rel_pg_space_id);
	if (!pageSpace->prefetchEnabled())
		return;

	PrefetchArray prf;

	if (!m_len || m_seqCheck < ppage->ppg_sequence)
	{
		m_seqCheck = m_seqDone = ppage->ppg_sequence;
		m_len = (m_len || m_seqCheck) ? 8 : 1;
		m_slotDone = slot + 8;
	}
	else
	{
		//const ULONG page_number = ppage->ppg_page[slot];
		//if (CCH_page_cached(tdbb, PageNumber(pageSpace->pageSpaceID, page_number)))
		//{
			if (m_len < 16)
				m_len *= 2;
		//}
			//else
		//{
		//	if (m_len > 2)
		//		m_len /= 2;
		//	//else
		//	//	m_slotDone++;
		//}
	}

	if (m_kind == FULLSCAN)
	{
		m_len = MAX(m_len, 16);
		int s = FB_ALIGN(m_slotDone + 1, 8);
		for (int i = 0; s <= ppage->ppg_count && i < m_len; s++)
		{
			if (s == ppage->ppg_count)
			{
				m_slotCheck = ppage->ppg_count;
				if (ppage->ppg_next)
					prf.push(ppage->ppg_next);
			}
			else if (ppage->ppg_page[s])
			{
				if (i == 0)
					m_slotCheck = s - 8;
				m_slotDone = s;
				prf.push(ppage->ppg_page[s]);
			}
			else
				continue;
			i++;
		}
	}
	else if (m_kind == PAGES_BITMAP)
	{
		PageBitmap::Accessor pages(m_pages);
		ULONG pageDone = m_slotDone + m_seqCheck * dbb->dbb_dp_per_pp;
		if (pages.locate(locGreat, pageDone))
		{
			for (int i = 0; i < m_len; i++)
			{
				ULONG pageno = pages.current();
				ULONG seq = pageno / dbb->dbb_dp_per_pp;

				if (seq != m_seqCheck)
				{
					if (relPages->rel_pages && seq < relPages->rel_pages->count())
						prf.push((*relPages->rel_pages)[seq]);
					break;
				}

				USHORT s = pageno % dbb->dbb_dp_per_pp;
				if (ppage->ppg_page[s])
				{
					if (i == 0)
						m_slotCheck = s;
					m_slotDone = s;
					prf.push(ppage->ppg_page[s]);
				}

				if (!pages.getNext())
					break;
			}
		}
		else
			m_slotDone = ppage->ppg_count;
	}
	else if (m_kind == RECS_BITMAP)
	{
		RecordNumber recDone;
		recDone.compose(dbb->dbb_max_records, dbb->dbb_dp_per_pp, 
			dbb->dbb_max_records, m_slotDone, m_seqDone);

		RecordBitmap::Accessor recs(m_recs);
		if (recs.locate(locGreatEqual, recDone.getValue()))
		{
			WIN pp2_window(relPages->rel_pg_space_id, ppage->ppg_header.pag_pageno);
			const pointer_page* pp2 = ppage;
			int pp2_seq = ppage->ppg_sequence;

			//m_len = 4;
			for (int i = 0; i < m_len; i++)
			{
				FB_UINT64 recno = recs.current();
				ULONG seq;
				USHORT s, l;
				{ // scope
					RecordNumber recnum(recno);
					recnum.decompose(dbb->dbb_max_records, dbb->dbb_dp_per_pp, l, s, seq);
				}

				if (seq != pp2_seq)
				{
					if (pp2 && pp2 != ppage)
						CCH_RELEASE(tdbb, &pp2_window);

					if (!relPages->rel_pages || seq >= relPages->rel_pages->count())
						break;

					ULONG pp2_page = (*relPages->rel_pages)[seq];

					if (CCH_page_cached(tdbb, PageNumber(pageSpace->pageSpaceID, pp2_page)) == pgMissing)
					{
						prf.push(pp2_page);
						break;
					}

					pp2_window.win_page = pp2_page;
					pp2 = (pointer_page*) CCH_FETCH_TIMEOUT(tdbb, &pp2_window, LCK_read, pag_undefined, 0);

					if (!pp2)
					{
						prf.push(pp2_page);
						break;
					}

					if (pp2->ppg_header.pag_type != pag_pointer ||
						pp2->ppg_relation != ppage->ppg_relation ||
						pp2->ppg_sequence != seq)
					{
						break;
					}
					pp2_seq = seq;
				}

				if (pp2->ppg_page[s])
				{
					if (i == 0)
					{
						m_slotCheck = s;
						m_seqCheck = seq;
					}
					m_slotDone = s;
					m_seqDone = pp2_seq;
					prf.push(pp2->ppg_page[s]);
				}

				recno += dbb->dbb_max_records - l;
				if (!recs.locate(locGreatEqual, recno))
					break;
			}

			if (pp2 && pp2 != ppage)
				CCH_RELEASE(tdbb, &pp2_window);
		}
/*
		else
		{
			m_slotDone = ppage->ppg_count;

			// prefetch adjacent pages if bitmap is singular

			if (recs.getFirst() && !recs.getNext())
			{
				const int ADJACENT_PAGES = 8;
				ULONG pageno = ppage->ppg_page[slot];
				if (CCH_page_cached(tdbb, PageNumber(pageSpace->pageSpaceID, pageno)) == pgMissing)
				{
					pageno = pageno & ~(ADJACENT_PAGES-1);
					for (int i = 0; i < ADJACENT_PAGES; i++, pageno++)
						prf.push(pageno);
				}
			}
		}
*/
	}

	if (prf.hasData())
	{
		pageSpace->registerPrefetch(prf);
/**
		extern PIORequest* CCH_make_PIO(thread_db* tdbb, PrefetchReq* prf);

		PrefetchReq* req = pageSpace->newPrefetchReq();
		if (req)
		{
			req->assign(prf);

			PIORequest* pio = CCH_make_PIO(tdbb, req);
			while (pio)
			{
				PIO_read_multy(dbb, &pio, 1);
				if (!pio || pio->readComplete(tdbb))
					break;
			}
		}
**/
	}
}


void DPMPrefetchInfo::nextDP(thread_db* tdbb, const RelationPages* relPages,
	const pointer_page* ppage, int slot)
{
	if (!m_enabled)
		return;

	if (m_len == 0)		// initialization
	{
		if (m_kind == FULLSCAN && ppage->ppg_count < 4)
		{
			m_enabled = false;
			return;
		}

		if (m_kind == RECS_BITMAP)
		{
			RecordBitmap::Accessor acc(m_recs);
			if (!acc.getFirst() || !acc.getNext())
			{
				m_enabled = false;
				return;
			}
		}

		m_len = 4;
		m_distance = m_len / 2;

		makePrefetch(tdbb, relPages, ppage);
		return;
	}

	Database* dbb = tdbb->getDatabase();
	ULONG currPage = ppage->ppg_sequence * dbb->dbb_dp_per_pp + slot;

	if (m_checkPage && currPage >= m_checkPage)
	{
		const PageNumber page(relPages->rel_pg_space_id, ppage->ppg_page[slot]);
		const PageCacheState state = CCH_page_cached(tdbb, page);
		switch (state)
		{
		case pgMissing:
			// prefetched page was preempted as it was never accessed, or
			// prefetch request still in queue
			if (m_distance > 0)
				m_distance--;
			else if (m_len > 4)
			{
				m_len -= 4;
				m_distance = m_len / 2;
			}
			break;

		case pgPending:
			// prefetched page still reading from disk
			if (m_distance < m_len - 1)
				m_distance++;
			else if (m_len > 4)
			{
				m_len -= 4;
				m_distance = m_distance > 2 ? m_distance - 2 : 0;
				if (m_distance >= m_len)
					m_distance = m_len - 1;
			}
			else
			{
				m_len = 2;
				m_distance = 0;
			}

//			if (m_trigPage && currPage >= m_trigPage)
//				m_trigPage = currPage + 1;
			break;

		case pgCached:
			if (m_len < m_maxLen)
			{
				if (m_len == 2)
				{
					m_len = 4;
					m_distance = 2;
				}
				else
				{
					m_len += 4;
					m_distance = MIN(m_len - 1, m_distance + 2);
				}
			}
			break;

		default:
			fb_assert(false);
			break;
		}

		m_checkPage = 0;
	}

	if (m_trigPage && currPage >= m_trigPage)
	{
		if (currPage > m_lastPage)
			m_lastPage = currPage;

		makePrefetch(tdbb, relPages, ppage);
	}
}

void DPMPrefetchInfo::makePrefetch(thread_db* tdbb, const RelationPages* relPages,
	const pointer_page* ppage)
{
	Database* dbb = tdbb->getDatabase();
	PrefetchArray prf;

	m_checkPage = 0;
	m_trigPage++;

	const pointer_page* pp2 = ppage;
	PointerPageHolder ppHolder(tdbb, relPages, ppage);

	// where to start new prefetch
	ULONG seq = (m_lastPage + 1) / dbb->dbb_dp_per_pp;
	int s = (m_lastPage + 1) % dbb->dbb_dp_per_pp - 1;

	RecordNumber recno;
	RecordBitmap::Accessor recs(m_recs);
	if (m_kind == RECS_BITMAP)
	{
		recno.setValue(dbb->dbb_max_records * (m_lastPage + 1));
	}

	for (ULONG len = 0; len < m_len; )
	{
		if (m_kind == RECS_BITMAP)
		{
			if (!recs.locate(locGreatEqual, recno.getValue()))
				break;

			recno.setValue(recs.current());

			USHORT slot, line;
			recno.decompose(dbb->dbb_max_records, dbb->dbb_dp_per_pp, line, slot, seq);
			s = slot;
		}

		if (seq != pp2->ppg_sequence)
			pp2 = ppHolder.fetch(seq);

		if (!pp2)
		{
			const ULONG pp2_page = ppHolder.getPageNum();
			if (pp2_page)
				prf.push(pp2_page);

			break;
		}

		if (m_kind == FULLSCAN)
		{
			for (s++; s < pp2->ppg_count; s++)
				if (pp2->ppg_page[s])		// TODO: check per-dp bits
					break;

			if (s == pp2->ppg_count)
			{
				s = -1;
				seq++;
				continue;
			}
		}
		else if (m_kind == RECS_BITMAP)
		{
			recno.compose(dbb->dbb_max_records, dbb->dbb_dp_per_pp, 0, s + 1, seq);
			if (!pp2->ppg_page[s])
				continue;
		}

		prf.push(pp2->ppg_page[s]);
		m_lastPage = seq * dbb->dbb_dp_per_pp + s;

		if (len == 0)
			m_checkPage = m_lastPage;

		if (len + 1 + m_distance == m_len)
			m_trigPage = m_lastPage;

		if (m_kind == FULLSCAN)
		{
			if (pp2 && (s + m_len >= pp2->ppg_count) && pp2->ppg_next)
				prf.push(pp2->ppg_next);
		}

		len++;
	}

	if (prf.hasData())
	{
		PageSpace* pageSpace = dbb->dbb_page_manager.findPageSpace(relPages->rel_pg_space_id);
		pageSpace->registerPrefetch(prf);
/*
		PrefetchReq* req = pageSpace->newPrefetchReq();
		if (req)
		{
			req->assign(prf);

			while (!req->isEmpty())
			{
				PIORequest* pio = CCH_make_PIO(tdbb, req);
				while (pio)
				{
					PIO_read_multy(dbb, &pio, 1);
					if (!pio || pio->readComplete(tdbb))
						break;
				}
			}
			pageSpace->freePrefetchReq(req);
		}
*/
	}
}

const pointer_page* DPMPrefetchInfo::PointerPageHolder::fetch(ULONG ppSequence)
{
	release();

	if (!m_relPages->rel_pages || ppSequence >= m_relPages->rel_pages->count())
	{
		m_window.win_page = 0;	// EOF
		return nullptr;
	}

	m_window.win_page = (*m_relPages->rel_pages)[ppSequence];

	if (CCH_page_cached(m_tdbb, m_window.win_page) == pgMissing)
		return nullptr;

	const pointer_page* ppage = (pointer_page*) CCH_FETCH_TIMEOUT(m_tdbb, &m_window, LCK_read, pag_undefined, 0);

	if (!ppage)
		return nullptr;

	if (ppage->ppg_header.pag_type != pag_pointer ||
		ppage->ppg_relation != m_ppage->ppg_relation ||
		ppage->ppg_sequence != ppSequence)
	{
		CCH_RELEASE(m_tdbb, &m_window);
		m_window.win_page = 0;
		return nullptr;
	}

	return ppage;
}


} // namespace Jrd
