#include "../jrd/BulkInsert.h"
#include "../jrd/cch.h"
#include "../jrd/pag.h"
#include "../jrd/sqz.h"
#include "../jrd/tra.h"
#include "../jrd/cch_proto.h"
#include "../jrd/dpm_proto.h"
#include "../jrd/ods_proto.h"
#include "../jrd/pag_proto.h"


using namespace Firebird;
using namespace Ods;

namespace Jrd
{

// How many bytes per record should be reserved, see SPACE_FUDGE in dpm.epp
constexpr unsigned RESERVE_SIZE = (ROUNDUP(RHDF_SIZE, ODS_ALIGNMENT) + sizeof(data_page::dpg_repeat));

BulkInsert::BulkInsert(MemoryPool& pool, const Database* dbb, jrd_rel* relation) :
	m_pool(pool),
	m_relation(relation),
	m_pageSize(dbb->dbb_page_size),
	m_spaceReserve((dbb->dbb_flags & DBB_no_reserve) ? 0 : RESERVE_SIZE),
	m_window(0, 0)
{
}

void BulkInsert::putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction)
{
	transaction->tra_flags |= TRA_write;

	rpb->rpb_b_page = 0;
	rpb->rpb_b_line = 0;
	rpb->rpb_flags = 0;
	rpb->rpb_transaction_nr = transaction->tra_number;

	Compressor dcc(m_pool, true, true, rpb->rpb_length, rpb->rpb_address);
	const ULONG packed = dcc.getPackedLength();

	const ULONG header_size = (transaction->tra_number > MAX_ULONG) ? RHDE_SIZE : RHD_SIZE;
	const ULONG max_data = m_pageSize - sizeof(data_page) - header_size;

	if (packed > max_data)
	{
		// store big
		fragmentRecord(tdbb, rpb, &dcc);
		return;
	}

	SLONG fill = (RHDF_SIZE - header_size) - packed;
	if (fill < 0)
		fill = 0;

	rhd* header = (rhd*) findSpace(tdbb, rpb, header_size + packed + fill);

	rpb->rpb_flags &= ~rpb_not_packed;

	header->rhd_flags = rpb->rpb_flags;
	Ods::writeTraNum(header, rpb->rpb_transaction_nr, header_size);
	header->rhd_format = rpb->rpb_format_number;

	fb_assert(rpb->rpb_b_page == 0);
	header->rhd_b_page = rpb->rpb_b_page;
	header->rhd_b_line = rpb->rpb_b_line;

	if (!dcc.isPacked())
		header->rhd_flags |= rhd_not_packed;

	UCHAR* const data = (UCHAR*) header + header_size;

	dcc.pack(rpb->rpb_address, data);

	if (fill)
		memset(data + packed, 0, fill);
}

void BulkInsert::fragmentRecord(thread_db* tdbb, record_param* rpb, Compressor* dcc)
{
	Database* dbb = tdbb->getDatabase();

	// Start compression from the end.

	const UCHAR* in = rpb->rpb_address + rpb->rpb_length;
	RelationPages* relPages = rpb->rpb_relation->getPages(tdbb);
	PageNumber prior(relPages->rel_pg_space_id, 0);

	// The last fragment should have rhd header because rhd_incomplete flag won't be set for it.
	// It's important for get_header() function which relies on rhd_incomplete flag to determine header size.
	ULONG header_size = RHD_SIZE;
	ULONG max_data = dbb->dbb_page_size - sizeof(data_page) - header_size;

	// Fill up data pages tail first until what's left fits on a single page.

	auto size = dcc->getPackedLength();
	fb_assert(size > max_data);

	do
	{
		// Allocate and format data page and fragment header

		data_page* page = (data_page*) DPM_allocate(tdbb, &rpb->getWindow(tdbb));

		page->dpg_header.pag_type = pag_data;
		page->dpg_header.pag_flags = dpg_orphan | dpg_full;
		page->dpg_relation = rpb->rpb_relation->rel_id;
		page->dpg_count = 1;

		const auto inLength = dcc->truncateTail(max_data);
		in -= inLength;
		size = dcc->getPackedLength();

		const Compressor tailDcc(tdbb, inLength, in);
		const auto tail_size = tailDcc.getPackedLength();
		fb_assert(tail_size <= max_data);

		// Cast to (rhdf*) but use only rhd fields for the last fragment
		rhdf* header = (rhdf*) &page->dpg_rpt[1];
		page->dpg_rpt[0].dpg_offset = (UCHAR*) header - (UCHAR*) page;
		page->dpg_rpt[0].dpg_length = tail_size + header_size;
		header->rhdf_flags = rhd_fragment;

		if (prior.getPageNum())
		{
			// This is not the last fragment
			header->rhdf_flags |= rhd_incomplete;
			header->rhdf_f_page = prior.getPageNum();
		}

		if (!tailDcc.isPacked())
			header->rhdf_flags |= rhd_not_packed;

		const auto out = (UCHAR*) header + header_size;
		tailDcc.pack(in, out);

		if (prior.getPageNum())
			CCH_precedence(tdbb, &rpb->getWindow(tdbb), prior);

		CCH_RELEASE(tdbb, &rpb->getWindow(tdbb));
		prior = rpb->getWindow(tdbb).win_page;

		// Other fragments except the last one should have rhdf header
		header_size = RHDF_SIZE;
		max_data = dbb->dbb_page_size - sizeof(data_page) - header_size;
	} while (size > max_data);

	// What's left fits on a page. Store it somewhere.

	const auto inLength = in - rpb->rpb_address;

	rhdf* header = (rhdf*) findSpace(tdbb, rpb, RHDF_SIZE + size);

	rpb->rpb_flags &= ~rpb_not_packed;

	header->rhdf_flags = rhd_incomplete | rhd_large | rpb->rpb_flags;
	Ods::writeTraNum(header, rpb->rpb_transaction_nr, RHDF_SIZE);
	header->rhdf_format = rpb->rpb_format_number;
	header->rhdf_b_page = rpb->rpb_b_page;
	header->rhdf_b_line = rpb->rpb_b_line;
	header->rhdf_f_page = prior.getPageNum();
	header->rhdf_f_line = 0;

	if (!dcc->isPacked())
		header->rhdf_flags |= rhd_not_packed;

	dcc->pack(rpb->rpb_address, header->rhdf_data);

	if (!(m_current->dpg_header.pag_flags & dpg_large))
	{
		const int bit = m_reserved - (m_lastReserved - m_window.win_page.getPageNum()) - 1;
		fb_assert(bit >= 0);
		fb_assert(bit < m_reserved);

		m_largeMask |= (1UL << bit);
		m_current->dpg_header.pag_flags |= dpg_large;
	}

	CCH_precedence(tdbb, &m_window, prior);
}

UCHAR* BulkInsert::findSpace(thread_db* tdbb, record_param* rpb, USHORT size)
{
	// record (with header) size, aligned up to ODS_ALIGNMENT
	const ULONG aligned = ROUNDUP(size, ODS_ALIGNMENT);

	// already used slots
	const ULONG used = (m_current ? m_current->dpg_count : 0);

	// size to allocate
	const ULONG alloc = aligned + (used ? sizeof(data_page::dpg_repeat) : 0);

	if (alloc + m_spaceReserve * (used + 1) > m_freeSpace)
	{
		if (m_current)
		{
			m_current->dpg_header.pag_flags |= dpg_full;

			// Get next reserved page, or reserve a new set of pages.
			const ULONG pageno = m_window.win_page.getPageNum();
			if (pageno < m_lastReserved)
			{
				const auto dpSequence = m_current->dpg_sequence;
				const auto count = m_current->dpg_count;

				CCH_MARK(tdbb, &m_window);
				CCH_RELEASE(tdbb, &m_window);

				tdbb->bumpRelStats(RuntimeStatistics::RECORD_INSERTS, m_relation->rel_id, count);

				m_window.win_page = pageno + 1;
				m_current = (data_page*) CCH_FETCH(tdbb, &m_window, LCK_write, pag_data);
			}
			else
			{
				flush(tdbb);
				m_current = nullptr;
			}
		}

		if (!m_current)
		{
			m_current = allocatePages(tdbb);
		}

		m_current->dpg_header.pag_flags |= dpg_swept;
		m_freeSpace = m_pageSize - sizeof(data_page);

		CCH_precedence(tdbb, &m_window, PageNumber(TRANS_PAGE_SPACE, rpb->rpb_transaction_nr));
	}

	fb_assert(alloc <= m_freeSpace);

	data_page::dpg_repeat* index = m_current->dpg_rpt + m_current->dpg_count;
	index->dpg_length = size;

	index->dpg_offset = (m_current->dpg_count > 0) ? index[-1].dpg_offset : m_pageSize;
	index->dpg_offset -= aligned;

	m_current->dpg_count++;
	m_freeSpace -= aligned + (used ? sizeof(data_page::dpg_repeat) : 0);

	Database* dbb = tdbb->getDatabase();
	rpb->rpb_number.setValue(dbb->dbb_max_records * m_current->dpg_sequence + m_current->dpg_count - 1);

	return reinterpret_cast<UCHAR*>(m_current) + index->dpg_offset;
}

data_page* BulkInsert::allocatePages(thread_db* tdbb)
{
	Database* dbb = tdbb->getDatabase();
	RelationPages* relPages = m_relation->getPages(tdbb);

	m_window.win_page.setPageSpaceID(relPages->rel_pg_space_id);
	m_reserved = DPM_reserve_pages(tdbb, m_relation, &m_window);

	fb_assert(m_reserved <= sizeof(m_largeMask) * 8);

	auto dpage = reinterpret_cast<data_page*>(m_window.win_buffer);

	m_lastReserved = m_window.win_page.getPageNum() + m_reserved - 1;
	m_firstSlot = dpage->dpg_sequence % dbb->dbb_dp_per_pp;
	m_largeMask = 0;

	return dpage;
}

void BulkInsert::flush(thread_db* tdbb)
{
	if (!m_current)
		return;

	Database* dbb = tdbb->getDatabase();

	const ULONG pp_sequence = m_current->dpg_sequence / dbb->dbb_dp_per_pp;
	const USHORT currSlot = m_current->dpg_sequence % dbb->dbb_dp_per_pp;

	const bool currFull = (m_current->dpg_header.pag_flags & dpg_full);
	const auto count = m_current->dpg_count;
	fb_assert(count > 0);

	CCH_MARK(tdbb, &m_window);
	CCH_RELEASE(tdbb, &m_window);
	m_current = nullptr;

	tdbb->bumpRelStats(RuntimeStatistics::RECORD_INSERTS, m_relation->rel_id, count);


	RelationPages* relPages = m_relation->getPages(tdbb);

	win ppWindow(relPages->rel_pg_space_id, (*relPages->rel_pages)[pp_sequence]);
	pointer_page* ppage = (pointer_page*) CCH_FETCH(tdbb, &ppWindow, LCK_write, pag_pointer);

	for (ULONG pageno = m_lastReserved - m_reserved + 1; pageno <= m_lastReserved; pageno++)
		CCH_precedence(tdbb, &ppWindow, pageno);

	CCH_MARK(tdbb, &ppWindow);

	UCHAR* bits = (UCHAR*) (ppage->ppg_page + dbb->dbb_dp_per_pp);
	ULONG largeMask = 1;
	for (USHORT slot = m_firstSlot; slot < m_firstSlot + m_reserved; slot++)
	{
		PPG_DP_BIT_CLEAR(bits, slot, ppg_dp_reserved);

		if (slot <= currSlot)
		{
			PPG_DP_BIT_CLEAR(bits, slot, ppg_dp_empty);
			PPG_DP_BIT_SET(bits, slot, ppg_dp_swept);

			if (slot < currSlot || currFull)
				PPG_DP_BIT_SET(bits, slot, ppg_dp_full);

			if (m_largeMask & largeMask)
				PPG_DP_BIT_SET(bits, slot, ppg_dp_large);
		}

		largeMask <<= 1;
	}

	CCH_RELEASE(tdbb, &ppWindow);
}

};	// namespace Jrd