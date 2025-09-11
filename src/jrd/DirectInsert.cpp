#include "../jrd/DirectInsert.h"
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

DirectInsert::DirectInsert(MemoryPool& pool, jrd_rel* relation, ULONG pageSize) :
	m_pool(pool),
	m_relation(relation),
	m_pageSize(pageSize),
	m_window(0, 0)
{
}

void DirectInsert::putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction)
{
	Compressor dcc(m_pool, true, true, rpb->rpb_length, rpb->rpb_address);
	const ULONG packed = dcc.getPackedLength();

	const ULONG header_size = (transaction->tra_number > MAX_ULONG) ? RHDE_SIZE : RHD_SIZE;
	const ULONG max_data = m_pageSize - sizeof(data_page) - header_size;

	if (packed > max_data)
	{
		// store big
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

UCHAR* DirectInsert::findSpace(thread_db* tdbb, record_param* rpb, USHORT size)
{
	// record (with header) size, aligned up to ODS_ALIGNMENT
	const ULONG aligned = ROUNDUP(size, ODS_ALIGNMENT);

	// size to allocate
	const ULONG alloc = (m_current->dpg_count ? 0 : sizeof(data_page::dpg_repeat)) +
		aligned + RESERVE_SIZE;

	if (alloc > m_freeSpace)
	{
		if (m_current)
		{
			m_current->dpg_header.pag_flags |= dpg_full;

			// Get next reserved page, or reserve a new set of pages.
			const ULONG pageno = m_window.win_page.getPageNum();
			if (pageno < m_lastReserved)
			{
				CCH_RELEASE(tdbb, &m_window);

				m_window.win_page = pageno + 1;
				m_current = (data_page*) CCH_fake(tdbb, &m_window, 1);
				m_current->dpg_header.pag_flags |= dpg_swept;
			}
			else
			{
				flush(tdbb);
				m_current = nullptr;
			}
		}

		if (!m_current)
			m_current = allocatePages(tdbb);

		m_freeSpace = m_pageSize - sizeof(data_page);
	}

	fb_assert(alloc <= m_freeSpace);

	data_page::dpg_repeat* index = m_current->dpg_rpt + m_current->dpg_count;
	index->dpg_length = size;

	index->dpg_offset = (m_current->dpg_count > 0) ? index[-1].dpg_offset : m_pageSize;
	index->dpg_offset -= aligned;

	m_current->dpg_count++;

	m_freeSpace -= alloc;

	return reinterpret_cast<UCHAR*>(m_current) + index->dpg_offset;
}

data_page* DirectInsert::allocatePages(thread_db* tdbb)
{
	Database* dbb = tdbb->getDatabase();
	RelationPages* relPages = m_relation->getPages(tdbb);

	m_window.win_page = (relPages->rel_pg_space_id, 0);
	m_reserved = DPM_reserve_pages(tdbb, m_relation, &m_window);

	auto dpage = reinterpret_cast<data_page*>(m_window.win_buffer);

	m_lastReserved = m_window.win_page.getPageNum() + m_reserved - 1;
	m_firstSlot = dpage->dpg_sequence % dbb->dbb_dp_per_pp;

	return dpage;
}

void DirectInsert::flush(thread_db* tdbb)
{
	Database* dbb = tdbb->getDatabase();

	const ULONG pp_sequence = m_current->dpg_sequence / dbb->dbb_dp_per_pp;
	const USHORT currSlot = m_current->dpg_sequence % dbb->dbb_dp_per_pp;
	const bool currFull = (m_current->dpg_header.pag_flags & dpg_full);
	fb_assert(m_current->dpg_count > 0);

 	CCH_RELEASE(tdbb, &m_window);
	m_current = nullptr;

	RelationPages* relPages = m_relation->getPages(tdbb);

	win ppWindow(relPages->rel_pg_space_id, (*relPages->rel_pages)[pp_sequence]);
	pointer_page* ppage = (pointer_page*) CCH_FETCH(tdbb, &ppWindow, LCK_write, pag_pointer);

	for (ULONG pageno = m_lastReserved - m_reserved + 1; pageno <= m_lastReserved; pageno++)
		CCH_precedence(tdbb, &ppWindow, pageno);

	CCH_MARK(tdbb, &ppWindow);

	UCHAR* bits = (UCHAR*) (ppage->ppg_page + dbb->dbb_dp_per_pp);
	for (USHORT slot = m_firstSlot; slot < m_firstSlot + m_reserved; slot++)
	{
		PPG_DP_BIT_CLEAR(bits, slot, ppg_dp_reserved);

		if (slot <= currSlot)
		{
			PPG_DP_BIT_CLEAR(bits, slot, ppg_dp_empty);
			PPG_DP_BIT_SET(bits, slot, ppg_dp_swept);

			if (slot < currSlot || currFull)
				PPG_DP_BIT_SET(bits, slot, ppg_dp_full);
		}
	}

	CCH_RELEASE(tdbb, &ppWindow);
}

};	// namespace Jrd