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
 *  Copyright (c) 2011 Vladyslav Khorsun <hvlad@users.sourceforge.net>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 */

#include "firebird.h"
#include <string.h>
#include "../../common/common.h"
#include "../../jrd/os/pio.h"
#include "../../jrd/cch.h"
#include "../../jrd/jrd.h"


namespace Jrd {

const ULONG BDBPageNum::generate(const BufferDesc* bdb) 
{ 
	return bdb->bdb_page.getPageNum(); 
}


/// class PIORequest

PIORequest::PIORequest(MemoryPool& pool) :
	m_next(NULL),
	m_file(NULL),
	m_state(PIOR_CREATED),
	m_osError(0),
	m_buffer(pool),
	m_aligned_buffer(NULL),
	m_pages(pool, MAX_PAGES_PER_PIOREQ/2),
#if defined (WIN_NT) || defined (USE_LIBAIO)
	m_segments(pool), 
#endif
	m_startIdx(0),
	m_count(0),
	m_ioSize(0)
{
	memset(&m_osData, 0, sizeof(m_osData));
}

void PIORequest::addPage(BufferDesc* bdb)
{
	m_pages.add(bdb);
}

int PIORequest::fitPageNumber(ULONG pageNumber, ULONG pageSize) const
{
	if (isEmpty())
		return 0;

	const ULONG MAX_GAP = MIN((OS_CACHED_IO_SIZE - 2 * pageSize) / pageSize, 2) + 1;

	if (pageNumber + MAX_GAP < minPage())
		return -1;

	if (pageNumber > maxPage() + MAX_GAP) 
		return 1;

	return 0;
}

bool PIORequest::isFull() const
{
	return (m_pages.getCount() == m_pages.getCapacity() ||
		maxPage() - minPage() >= MAX_PAGES_PER_PIOREQ);
}

ULONG PIORequest::minPage() const
{
	return isEmpty() ? 0 : (*m_pages.begin())->bdb_page.getPageNum();
}

ULONG PIORequest::maxPage() const
{
	return isEmpty() ? 0 : (*(m_pages.end() - 1))->bdb_page.getPageNum();
}

void* PIORequest::allocIOBuffer(int sysPageSize)
{
	if (!m_aligned_buffer)
	{
		m_aligned_buffer = m_buffer.getBuffer(OS_CACHED_IO_SIZE + sysPageSize);
		m_aligned_buffer = (SCHAR*) FB_ALIGN((IPTR) m_aligned_buffer, sysPageSize);
	}
	return m_aligned_buffer;
}


bool PIORequest::readComplete(thread_db* tdbb)
{
	Database* dbb = tdbb->getDatabase();

	const SCHAR* ioBuffer = (SCHAR*) getIOBuffer();
	ULONG prevPageNo = m_pages[m_startIdx]->bdb_page.getPageNum() - 1;

	for (unsigned int i = m_startIdx; i < m_startIdx + m_count; i++)
	{
		BufferDesc* bdb = m_pages[i];
		if (ioBuffer)
		{
			// account gap between prior page and current one
			ULONG pageNo = bdb->bdb_page.getPageNum();
			int gap = pageNo - prevPageNo - 1;
			if (gap > 0)
				ioBuffer += gap * dbb->dbb_page_size;
			prevPageNo = pageNo;
		}

		bdb->readComplete(tdbb, m_state != PIORequest::PIOR_COMPLETED, ioBuffer);

		if (ioBuffer)
			ioBuffer += dbb->dbb_page_size;
	}

	tdbb->bumpStats(RuntimeStatistics::PAGE_READS_MULTY_CNT);

	tdbb->bumpStats(RuntimeStatistics::PAGE_READS_MULTY_PAGES, m_count);

	tdbb->bumpStats(RuntimeStatistics::PAGE_READS_MULTY_IOSIZE, 
		m_pages[m_startIdx + m_count - 1]->bdb_page.getPageNum() - 
		m_pages[m_startIdx]->bdb_page.getPageNum() + 1);

	if (m_state == PIORequest::PIOR_COMPLETED && 
		m_startIdx + m_count < m_pages.getCount())
	{
		m_startIdx += m_count;
		m_count = 0;
		return false;
	}

	BufferDesc* bdb = m_pages[0];
	PageManager& pageMgr = dbb->dbb_page_manager;
	PageSpace* pageSpace = pageMgr.findPageSpace(bdb->bdb_page.getPageSpaceID());
	pageSpace->freePIORequest(this);
	return true;
}

Firebird::string PIORequest::dump() const
{
	char* states[4] = {"PIOR_CREATED", "PIOR_PENDING", "PIOR_COMPLETED", "PIOR_ERROR"};

	Firebird::string s, s1;
	s.printf("pio 0x%x, start %6d, size %6d, state %s, bdbs :",
		this, m_startIdx, m_ioSize, states[m_state]);

	for (unsigned int i = 0; i < m_pages.getCount(); i++)
	{
		BufferDesc* bdb = m_pages[i];
		s1.printf(" %6d", bdb->bdb_page.getPageNum());
		s.append(s1);
	}
	s.append("\n");

	return s;
}

} // namespace Jrd
