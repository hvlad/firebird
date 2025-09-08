#include "../jrd/DirectInsert.h"
#include "../jrd/cch.h"
#include "../jrd/jrd.h"
#include "../jrd/pag.h"
#include "../jrd/sqz.h"
#include "../jrd/tra.h"
#include "../jrd/cch_proto.h"
#include "../jrd/ods_proto.h"
#include "../jrd/pag_proto.h"

using namespace Firebird;
using namespace Ods;

namespace Jrd
{

constexpr unsigned PRIMARY_PAGES = 8;
constexpr unsigned FRAGMENT_PAGES = 8;

// How many bytes per record should be reserved, see SPACE_FUDGE in dpm.epp
constexpr unsigned RESERVE_SIZE = (ROUNDUP(RHDF_SIZE, ODS_ALIGNMENT) + sizeof(data_page::dpg_repeat));

DirectInsert::DirectInsert(MemoryPool& pool, jrd_rel* relation, ULONG pageSize) :
	m_pool(pool),
	m_relation(relation),
	m_pageSize(pageSize),
	m_primary(pool),
	m_fragments(pool)
{
	m_dp = m_primary.getAlignedBuffer(m_pageSize * PRIMARY_PAGES, m_pageSize);
	m_frgm = nullptr;

	m_current = nullptr;;
	m_freeSpace = 0;
}

/*****

store
  if big
    store tail
	rec = head

  find space
    if !page || size > free
	  if next
		page = next
	  else
	    flush
		page = begin()
	free = maxfree
	page->index[page->count].len = size
	space = page->index[page->count].offset = prev_offset - aligned_size

  put rec

****/

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

void DirectInsert::allocatePages(thread_db* tdbb, unsigned count, UCHAR* ptr)
{
	RelationPages* relPages = m_relation->getPages(tdbb);

	win window(relPages->rel_pg_space_id, 0);
	PAG_allocate_pages(tdbb, &window, count, true);
	CCH_RELEASE(tdbb, &window);

	ULONG pageno = window.win_page.getPageNum();

	// "count" pages starting from "pageno" is reserved now, nobody can use it as
	// there is no path to it

	for (int i = 0; i < count; i++)
	{
		data_page* page = reinterpret_cast<data_page*>(ptr);
		page->dpg_header.pag_type = pag_data;
		page->dpg_header.pag_flags = 0;
		page->dpg_relation = m_relation->rel_id;
		page->dpg_count = 0;
		page->dpg_sequence = 0;

		// save page number
		page->dpg_header.pag_pageno = pageno++;

		ptr += m_pageSize;
	}
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
			m_current->dpg_header.pag_flags |= dpg_full | dpg_swept;

			// use next page in buffer
			UCHAR* const ptr = reinterpret_cast<UCHAR*>(m_current) + m_pageSize;
			if (ptr < m_primary.end())
				m_current = reinterpret_cast<data_page*>(ptr);
			else
			{
				flush(tdbb);
				m_current = nullptr;
			}
		}

		if (!m_current)
		{
			allocatePages(tdbb, PRIMARY_PAGES, m_dp);
			m_current = reinterpret_cast<data_page*>(m_dp);
		}

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

void DirectInsert::flush(thread_db* tdbb)
{
	// write fragment pages
	// write non-empty data pages
	// put DP numbers into PP
	// write PP

	// make sure no written pages have its wrong copy in the cache!!!
}

};	// namespace Jrd