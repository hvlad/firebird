/*
 *	PROGRAM:	JRD Access Method
 *	MODULE:		winnt.cpp
 *	DESCRIPTION:	Windows NT specific physical IO
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
 * 2001.07.06 Sean Leyne - Code Cleanup, removed "#ifdef READONLY_DATABASE"
 *                         conditionals, as the engine now fully supports
 *                         readonly databases.
 *
 *
 * 17-Oct-2001 Mike Nordell: Non-shared file access
 *
 * 02-Nov-2001 Mike Nordell: Synch with FB1 changes.
 *
 * 20-Nov-2001 Ann Harrison: Make page count work on db with forced write
 *
 * 21-Nov-2001 Ann Harrison: Allow read sharing so gstat works
 */

#include "firebird.h"
#include <string.h>
#include "../jrd/jrd.h"
#include "../jrd/os/pio.h"
#include "../jrd/ods.h"
#include "../jrd/lck.h"
#include "../jrd/cch.h"
#include "gen/iberror.h"
#include "../jrd/cch_proto.h"
#include "../jrd/err_proto.h"
#include "../common/isc_proto.h"
#include "../common/isc_f_proto.h"

#include "../jrd/lck_proto.h"
#include "../jrd/mov_proto.h"
#include "../jrd/os/pio_proto.h"
#include "../common/classes/init.h"
#include "../common/config/config.h"

#include <windows.h>

#ifdef WIN9X_SUPPORT
#include "win9x_nt.h"
#endif

namespace Jrd {

class FileExtendLockGuard
{
public:
	FileExtendLockGuard(Firebird::RWLock* lock, bool exclusive) :
	  m_lock(lock), m_exclusive(exclusive)
	{
		if (m_exclusive) {
			fb_assert(m_lock);
		}
		if (m_lock)
		{
			if (m_exclusive)
				m_lock->beginWrite(FB_FUNCTION);
			else
				m_lock->beginRead(FB_FUNCTION);
		}
	}

	~FileExtendLockGuard()
	{
		if (m_lock)
		{
			if (m_exclusive)
				m_lock->endWrite();
			else
				m_lock->endRead();
		}
	}

private:
	// copying is prohibited
	FileExtendLockGuard(const FileExtendLockGuard&);
	FileExtendLockGuard& operator=(const FileExtendLockGuard&);

	Firebird::RWLock* const m_lock;
	const bool m_exclusive;
};


} // namespace Jrd

using namespace Jrd;
using namespace Firebird;

#ifdef TEXT
#undef TEXT
#endif
#define TEXT		SCHAR

static bool	maybeCloseFile(HANDLE&);
static jrd_file* seek_file(jrd_file*, BufferDesc*, FbStatusVector*, OVERLAPPED*);
static jrd_file* setup_file(Database*, const Firebird::PathName&, HANDLE, bool, bool);
static bool nt_error(const TEXT*, const jrd_file*, ISC_STATUS, FbStatusVector* const);
static void adjustFileSystemCacheSize();

struct AdjustFsCache
{
	static void init() { adjustFileSystemCacheSize(); }
};

static InitMutex<AdjustFsCache> adjustFsCacheOnce("AdjustFsCacheOnce");

inline static DWORD getShareFlags(const bool shared_access, bool temporary = false)
{
	return FILE_SHARE_READ | ((!temporary && shared_access) ? FILE_SHARE_WRITE : 0);
}

static const DWORD g_dwExtraFlags = FILE_FLAG_OVERLAPPED;

static const DWORD g_dwExtraTempFlags = FILE_ATTRIBUTE_TEMPORARY |
										FILE_FLAG_DELETE_ON_CLOSE;


int PIO_add_file(thread_db* tdbb, jrd_file* main_file, const Firebird::PathName& file_name, SLONG start)
{
/**************************************
 *
 *	P I O _ a d d _ f i l e
 *
 **************************************
 *
 * Functional description
 *	Add a file to an existing database.  Return the sequence
 *	number of the new file.  If anything goes wrong, return a
 *	sequence of 0.
 *
 **************************************/
	jrd_file* const new_file = PIO_create(tdbb, file_name, false, false);
	if (!new_file)
		return 0;

	new_file->fil_min_page = start;
	USHORT sequence = 1;

	jrd_file* file;
	for (file = main_file; file->fil_next; file = file->fil_next)
		++sequence;

	file->fil_max_page = start - 1;
	file->fil_next = new_file;

	return sequence;
}


void PIO_close(jrd_file* main_file)
{
/**************************************
 *
 *	P I O _ c l o s e
 *
 **************************************
 *
 * Functional description
 *
 **************************************/
	for (jrd_file* file = main_file; file; file = file->fil_next)
	{
		if (maybeCloseFile(file->fil_desc))
		{
		}
	}
}


jrd_file* PIO_create(thread_db* tdbb, const Firebird::PathName& string,
					 const bool overwrite, const bool temporary)
{
/**************************************
 *
 *	P I O _ c r e a t e
 *
 **************************************
 *
 * Functional description
 *	Create a new database file.
 *
 **************************************/
	adjustFsCacheOnce.init();

	Database* const dbb = tdbb->getDatabase();

	const TEXT* file_name = string.c_str();
	const bool shareMode = dbb->dbb_config->getServerMode() != MODE_SUPER;

	DWORD dwShareMode = getShareFlags(shareMode, temporary);

	DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL | g_dwExtraFlags;
	if (temporary)
		dwFlagsAndAttributes |= g_dwExtraTempFlags;

	const HANDLE desc = CreateFile(file_name,
					  GENERIC_READ | GENERIC_WRITE,
					  dwShareMode,
					  NULL,
					  (overwrite ? CREATE_ALWAYS : CREATE_NEW),
					  dwFlagsAndAttributes,
					  0);

	if (desc == INVALID_HANDLE_VALUE)
	{
		ERR_post(Arg::Gds(isc_io_error) << Arg::Str("CreateFile (create)") << Arg::Str(string) <<
				 Arg::Gds(isc_io_create_err) << Arg::Windows(GetLastError()));
	}

	// File open succeeded.  Now expand the file name.
	// workspace is the expanded name here

	Firebird::PathName workspace(string);
	ISC_expand_filename(workspace, false);

	return setup_file(dbb, workspace, desc, false, shareMode);
}


bool PIO_expand(const TEXT* file_name, USHORT file_length, TEXT* expanded_name, FB_SIZE_T len_expanded)
{
/**************************************
 *
 *	P I O _ e x p a n d
 *
 **************************************
 *
 * Functional description
 *	Fully expand a file name.  If the file doesn't exist, do something
 *	intelligent.
 *
 **************************************/

	return ISC_expand_filename(file_name, file_length, expanded_name, len_expanded, false);
}


void PIO_extend(thread_db* tdbb, jrd_file* main_file, const ULONG extPages, const USHORT pageSize)
{
/**************************************
 *
 *	P I O _ e x t e n d
 *
 **************************************
 *
 * Functional description
 *	Extend file by extPages pages of pageSize size.
 *
 **************************************/
	// hvlad: prevent other reading\writing threads from changing file pointer.
	// As we open file without FILE_FLAG_OVERLAPPED, ReadFile\WriteFile calls
	// will change file pointer we set here and file truncation instead of file
	// extension will occurs.
	// It solved issue CORE-1468 (database file corruption when file extension
	// and read\write activity performed simultaneously)

	// if file have no extend lock it is better to not extend file than corrupt it
	if (!main_file->fil_ext_lock)
		return;

	EngineCheckout cout(tdbb, FB_FUNCTION, true);
	FileExtendLockGuard extLock(main_file->fil_ext_lock, true);

	ULONG leftPages = extPages;
	for (jrd_file* file = main_file; file && leftPages; file = file->fil_next)
	{
		const ULONG filePages = PIO_get_number_of_pages(file, pageSize);
		const ULONG fileMaxPages = (file->fil_max_page == MAX_ULONG) ?
									MAX_ULONG : file->fil_max_page - file->fil_min_page + 1;
		if (filePages < fileMaxPages)
		{
			const ULONG extendBy = MIN(fileMaxPages - filePages + file->fil_fudge, leftPages);

			HANDLE hFile = file->fil_desc;

			LARGE_INTEGER newSize;
			newSize.QuadPart = ((ULONGLONG) filePages + extendBy) * pageSize;

			const DWORD ret = SetFilePointer(hFile, newSize.LowPart, &newSize.HighPart, FILE_BEGIN);
			if (ret == INVALID_SET_FILE_POINTER && GetLastError() != NO_ERROR) {
				nt_error("SetFilePointer", file, isc_io_write_err, 0);
			}
			if (!SetEndOfFile(hFile)) {
				nt_error("SetEndOfFile", file, isc_io_write_err, 0);
			}

			leftPages -= extendBy;
		}
	}
}


void PIO_flush(thread_db* tdbb, jrd_file* main_file)
{
/**************************************
 *
 *	P I O _ f l u s h
 *
 **************************************
 *
 * Functional description
 *	Flush the operating system cache back to good, solid oxide.
 *
 **************************************/
	EngineCheckout cout(tdbb, FB_FUNCTION, true);

	for (jrd_file* file = main_file; file; file = file->fil_next)
		FlushFileBuffers(file->fil_desc);
}


void PIO_force_write(jrd_file* file, const bool forceWrite, const bool notUseFSCache)
{
/**************************************
 *
 *	P I O _ f o r c e _ w r i t e
 *
 **************************************
 *
 * Functional description
 *	Set (or clear) force write, if possible, for the database.
 *
 **************************************/

	const bool oldForce = (file->fil_flags & FIL_force_write) != 0;
	const bool oldNotUseCache = (file->fil_flags & FIL_no_fs_cache) != 0;

	if (forceWrite != oldForce || notUseFSCache != oldNotUseCache)
	{
		const int force = forceWrite ? FILE_FLAG_WRITE_THROUGH : 0;
		const int fsCache = notUseFSCache ? FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED : 0;
		const int writeMode = (file->fil_flags & FIL_readonly) ? 0 : GENERIC_WRITE;
		const bool sharedMode = (file->fil_flags & FIL_sh_write);

        HANDLE& hFile = file->fil_desc;
		maybeCloseFile(hFile);
		hFile = CreateFile(file->fil_string,
						  GENERIC_READ | writeMode,
						  getShareFlags(sharedMode),
						  NULL,
						  OPEN_EXISTING,
						  FILE_ATTRIBUTE_NORMAL | force | fsCache | g_dwExtraFlags,
						  0);

		if (hFile == INVALID_HANDLE_VALUE)
		{
			ERR_post(Arg::Gds(isc_io_error) << Arg::Str("CreateFile (force write)") <<
											   Arg::Str(file->fil_string) <<
					 Arg::Gds(isc_io_access_err) << Arg::Windows(GetLastError()));
		}

		if (forceWrite) {
			file->fil_flags |= FIL_force_write;
		}
		else {
			file->fil_flags &= ~FIL_force_write;
		}
		if (notUseFSCache) {
			file->fil_flags |= FIL_no_fs_cache;
		}
		else {
			file->fil_flags &= ~FIL_no_fs_cache;
		}

		file->fil_flags &= ~FIL_aio_init;
	}
}


void PIO_header(thread_db* tdbb, UCHAR* address, int length)
{
/**************************************
 *
 *	P I O _ h e a d e r
 *
 **************************************
 *
 * Functional description
 *	Read the page header.  This assumes that the file has not been
 *	repositioned since the file was originally mapped.
 *  The detail of Win32 implementation is that it doesn't assume
 *  this fact as seeks to first byte of file initially, but external
 *  callers should not rely on this behavior
 *
 **************************************/
	Database* const dbb = tdbb->getDatabase();

	PageSpace* pageSpace = dbb->dbb_page_manager.findPageSpace(DB_PAGE_SPACE);
	jrd_file* file = pageSpace->file;
	HANDLE desc = file->fil_desc;

	OVERLAPPED overlapped;
	memset(&overlapped, 0, sizeof(OVERLAPPED));

	ThreadSync* thread = ThreadSync::getThread("PIO_header");
	overlapped.hEvent = (HANDLE) ((UINT_PTR) thread->getIOEvent() | 1);

    DWORD actual_length;

	if (!ReadFile(desc, address, length, &actual_length, &overlapped) ||
		actual_length != (DWORD) length)
	{
		const DWORD dwError = GetLastError();
		if (dwError != ERROR_IO_PENDING ||
			!GetOverlappedResult(desc, &overlapped, &actual_length, TRUE) ||
			actual_length != length)
		{
			nt_error("ReadFile", file, isc_io_read_err, 0);
		}
	}
}

// we need a class here only to return memory on shutdown and avoid
// false memory leak reports
static Firebird::InitInstance<ZeroBuffer> zeros;


USHORT PIO_init_data(thread_db* tdbb, jrd_file* main_file, FbStatusVector* status_vector,
					 ULONG startPage, USHORT initPages)
{
/**************************************
 *
 *	P I O _ i n i t _ d a t a
 *
 **************************************
 *
 * Functional description
 *	Initialize tail of file with zeros
 *
 **************************************/
	const char* const zero_buff = zeros().getBuffer();
	const FB_SIZE_T zero_buff_size = zeros().getSize();

	Database* const dbb = tdbb->getDatabase();

	EngineCheckout cout(tdbb, FB_FUNCTION, true);
	FileExtendLockGuard extLock(main_file->fil_ext_lock, false);

	// Fake buffer, used in seek_file. Page space ID doesn't matter there
	// as we already know file to work with
	BufferDesc bdb(dbb->dbb_bcb);
	bdb.bdb_page = PageNumber(0, startPage);

	OVERLAPPED overlapped;
	jrd_file* file = seek_file(main_file, &bdb, status_vector, &overlapped);

	if (!file)
		return 0;

	if (file->fil_min_page + 8 > startPage)
		return 0;

	ThreadSync* thread = ThreadSync::getThread("PIO_init_data");

	USHORT leftPages = initPages;
	const ULONG initBy = MIN(file->fil_max_page - startPage, leftPages);
	if (initBy < leftPages)
		leftPages = initBy;

	for (ULONG i = startPage; i < startPage + initBy; )
	{
		bdb.bdb_page = PageNumber(0, i);
		USHORT write_pages = zero_buff_size / dbb->dbb_page_size;
		if (write_pages > leftPages)
			write_pages = leftPages;

		jrd_file* file1 = seek_file(main_file, &bdb, status_vector, &overlapped);
		fb_assert(file1 == file);

		overlapped.hEvent = (HANDLE) ((UINT_PTR) thread->getIOEvent() | 1);

		const DWORD to_write = (DWORD) write_pages * dbb->dbb_page_size;
		DWORD written;

		if (!WriteFile(file->fil_desc, zero_buff, to_write, &written, &overlapped) ||
			to_write != written)
		{
			const DWORD dwError = GetLastError();
			if (dwError != ERROR_IO_PENDING ||
				!GetOverlappedResult(file->fil_desc, &overlapped, &written, TRUE) ||
				to_write != written)
			{
				nt_error("WriteFile", file, isc_io_write_err, status_vector);
				break;
			}
		}

		leftPages -= write_pages;
		i += write_pages;
	}

	return (initPages - leftPages);
}


jrd_file* PIO_open(thread_db* tdbb,
				   const Firebird::PathName& string,
				   const Firebird::PathName& file_name)
{
/**************************************
 *
 *	P I O _ o p e n
 *
 **************************************
 *
 * Functional description
 *	Open a database file.
 *
 **************************************/
	Database* const dbb = tdbb->getDatabase();

	const TEXT* const ptr = (string.hasData() ? string : file_name).c_str();
	bool readOnly = false;
	const bool shareMode = dbb->dbb_config->getServerMode() != MODE_SUPER;

	adjustFsCacheOnce.init();

	HANDLE desc = CreateFile(ptr,
					  GENERIC_READ | GENERIC_WRITE,
					  getShareFlags(shareMode),
					  NULL,
					  OPEN_EXISTING,
					  FILE_ATTRIBUTE_NORMAL |
					  g_dwExtraFlags,
					  0);

	if (desc == INVALID_HANDLE_VALUE)
	{
		// Try opening the database file in ReadOnly mode.
		// The database file could be on a RO medium (CD-ROM etc.).
		// If this fileopen fails, return error.
		desc = CreateFile(ptr,
						  GENERIC_READ,
						  FILE_SHARE_READ,
						  NULL,
						  OPEN_EXISTING,
						  FILE_ATTRIBUTE_NORMAL |
						  g_dwExtraFlags, 0);

		if (desc == INVALID_HANDLE_VALUE)
		{
			ERR_post(Arg::Gds(isc_io_error) << Arg::Str("CreateFile (open)") << Arg::Str(file_name) <<
					 Arg::Gds(isc_io_open_err) << Arg::Windows(GetLastError()));
		}
		else
		{
			// If this is the primary file, set Database flag to indicate that it is
			// being opened ReadOnly. This flag will be used later to compare with
			// the Header Page flag setting to make sure that the database is set ReadOnly.
			readOnly = true;
			PageSpace* pageSpace = dbb->dbb_page_manager.findPageSpace(DB_PAGE_SPACE);
			if (!pageSpace->file)
				dbb->dbb_flags |= DBB_being_opened_read_only;
		}
	}

	return setup_file(dbb, string, desc, readOnly, shareMode);
}


bool PIO_read(thread_db* tdbb, jrd_file* file, BufferDesc* bdb, Ods::pag* page, FbStatusVector* status_vector)
{
/**************************************
 *
 *	P I O _ r e a d
 *
 **************************************
 *
 * Functional description
 *	Read a data page.
 *
 **************************************/
	Database* const dbb = tdbb->getDatabase();

	const DWORD size = dbb->dbb_page_size;

	EngineCheckout cout(tdbb, FB_FUNCTION, true);
	FileExtendLockGuard extLock(file->fil_ext_lock, false);

	OVERLAPPED overlapped;
	if (!(file = seek_file(file, bdb, status_vector, &overlapped)))
		return false;

	HANDLE desc = file->fil_desc;

	// This read request should be processed syncronously, i.e. it should not be
	// handled via completion port. 
	// See: http://msdn.microsoft.com/en-us/library/windows/desktop/aa364986.aspx :
	// A valid event handle whose low-order bit is set keeps I/O completion from being 
	// queued to the completion port.

	ThreadSync* thread = ThreadSync::getThread("PIO_read");
	overlapped.hEvent = (HANDLE) ((UINT_PTR) thread->getIOEvent() | 1);

	DWORD actual_length;

	if (!ReadFile(desc, page, size, &actual_length, &overlapped) ||
		actual_length != size)
	{
		const DWORD dwError = GetLastError();
		if (dwError != ERROR_IO_PENDING ||
			!GetOverlappedResult(desc, &overlapped, &actual_length, TRUE) ||
			actual_length != size)
		{
			return nt_error("ReadFile", file, isc_io_read_err, status_vector);
		}
	}

	return true;
}

class SysInfo
{
public:
	SysInfo(MemoryPool& pool) :
		buffer(pool)
	{
		GetSystemInfo(&si);
		sysPageBuffer = buffer.getBuffer(si.dwPageSize * 2);
		sysPageBuffer = (SCHAR*) FB_ALIGN((IPTR) sysPageBuffer, si.dwPageSize);
	}

	SYSTEM_INFO si;
	SCHAR* sysPageBuffer;

private:
	Array<SCHAR> buffer;
};

Firebird::InitInstance<SysInfo> sysInfo;


bool PIORequest::postRead(Database* dbb)
{
	if (m_file->fil_ext_lock)
		m_file->fil_ext_lock->beginRead(FB_FUNCTION);

	BOOL ret = FALSE;

	m_state = PIORequest::PIOR_PENDING;
	m_osError = 0;
	m_count = 0;
	m_ioSize = 0;

	unsigned int i = m_startIdx;
	ULONG firstPageNo = m_pages[i]->bdb_page.getPageNum();
	ULONG lastPageNo = firstPageNo;
	ULONG prevPageNo = firstPageNo - 1;

	jrd_file* file = seek_file(m_file, m_pages[i], NULL, &m_osData);
	dbb->dbb_page_manager.pioPort.addFile(file);

	if (file->fil_flags & FIL_no_fs_cache)
	{
		const int sysPageSize = sysInfo().si.dwPageSize;
		SCHAR* dummyBuffer = sysInfo().sysPageBuffer;

		int sysPagesPerBDB = dbb->dbb_page_size / sysPageSize;
		int segCnt = sysPagesPerBDB * (maxPage() - firstPageNo + 1);
		FILE_SEGMENT_ELEMENT *segArray = m_segments.getBuffer(segCnt + 1);
		FILE_SEGMENT_ELEMENT *currSeg = segArray;

		for (; i < m_pages.getCount(); i++)
		{
			BufferDesc* bdb = m_pages[i];
			ULONG pageNo = bdb->bdb_page.getPageNum();

			// fill gap between prior page and current one
			int gap = sysPagesPerBDB * (pageNo - prevPageNo - 1);
			if (gap > 0) 
			{
				for (int s = 0; s < gap; s++)
				{
					currSeg->Buffer = dummyBuffer;
					currSeg++;
				}
			}

			SCHAR* p = (SCHAR*) bdb->bdb_buffer;
			for (int s = 0; s < sysPagesPerBDB; s++)
			{
				currSeg->Buffer = p;
				currSeg++;
				p += sysPageSize;
			}

			lastPageNo = prevPageNo = pageNo;
			m_count++;
		}
		currSeg->Buffer = NULL;
		fb_assert(currSeg <= segArray + segCnt);

		m_ioSize = dbb->dbb_page_size * (lastPageNo - firstPageNo + 1);

		ret = ReadFileScatter(file->fil_desc, segArray, m_ioSize, NULL, &m_osData);
	}
	else
	{
		int pagesInBuffer = OS_CACHED_IO_SIZE / dbb->dbb_page_size;
		for (; i < m_pages.getCount() && pagesInBuffer; i++)
		{
			BufferDesc* bdb = m_pages[i];
			ULONG pageNo = bdb->bdb_page.getPageNum();

			// account gap between prior page and current one
			int gap = pageNo - prevPageNo - 1;
			if (gap > 0) 
			{
				if (gap > pagesInBuffer - 1)
					break;
				pagesInBuffer -= gap;
				m_ioSize += gap * dbb->dbb_page_size;
			}
			pagesInBuffer--;

			lastPageNo = prevPageNo = pageNo;
			m_count++;
			m_ioSize += dbb->dbb_page_size;
		}
		fb_assert(m_ioSize <= OS_CACHED_IO_SIZE);

		void* ioBuffer = m_pages[m_startIdx]->bdb_buffer;
		if (m_count > 1) 
			ioBuffer = allocIOBuffer(sysInfo().si.dwPageSize);

		ret = ReadFile(file->fil_desc, ioBuffer, m_ioSize, NULL, &m_osData);
	}

	PIORequest::PIOR_STATE newState = PIORequest::PIOR_PENDING;
	if (ret) 
	{
/***
		DWORD readBytes;
		if (GetOverlappedResult(m_file->fil_desc, &m_osData, &readBytes, FALSE))
		{
			if (readBytes == m_ioSize)
				newState = PIORequest::PIOR_COMPLETED;
			else
				newState = PIORequest::PIOR_ERROR;
		}
***/
	}
	else
	{
		DWORD err = GetLastError();
		if (err != ERROR_IO_PENDING)
			newState = PIORequest::PIOR_ERROR;
	}

	if (newState == PIORequest::PIOR_PENDING)
		return true;

	m_state = newState;
	if (m_file->fil_ext_lock)
		m_file->fil_ext_lock->endRead();

	return false;
}


void PIO_read_multy(Database* dbb, PIORequest** pReqs, int cnt)
{
	PIORequest** reqPtr = pReqs;
	PIORequest** end = pReqs + cnt;
	for (; reqPtr < end; reqPtr++)
	{
		PIORequest* req = *reqPtr;
		if (req->postRead(dbb))
			*reqPtr = NULL;
	}
}


bool PIO_write(thread_db* tdbb, jrd_file* file, BufferDesc* bdb, Ods::pag* page, FbStatusVector* status_vector)
{
/**************************************
 *
 *	P I O _ w r i t e
 *
 **************************************
 *
 * Functional description
 *	Write a data page.
 *
 **************************************/
	OVERLAPPED overlapped;

	Database* const dbb = tdbb->getDatabase();

	const DWORD size = dbb->dbb_page_size;

	EngineCheckout cout(tdbb, FB_FUNCTION, true);
	FileExtendLockGuard extLock(file->fil_ext_lock, false);

	file = seek_file(file, bdb, status_vector, &overlapped);
	if (!file)
		return false;

	HANDLE desc = file->fil_desc;

	// See comments at PIO_read
	ThreadSync* thread = ThreadSync::getThread("PIO_write");
	overlapped.hEvent = (HANDLE) ((UINT_PTR) thread->getIOEvent() | 1);

	DWORD actual_length;

	if (!WriteFile(desc, page, size, &actual_length, &overlapped) || actual_length != size )
	{
		const DWORD dwError = GetLastError();
		if (dwError != ERROR_IO_PENDING ||
			!GetOverlappedResult(desc, &overlapped, &actual_length, TRUE) ||
			actual_length != size)
		{
			return nt_error("WriteFile", file, isc_io_write_err, status_vector);
		}
	}

	return true;
}


ULONG PIO_get_number_of_pages(const jrd_file* file, const USHORT pagesize)
{
/**************************************
 *
 *	P I O _ g e t _ n u m b e r _ o f _ p a g e s
 *
 **************************************
 *
 * Functional description
 *	Compute number of pages in file, based only on file size.
 *
 **************************************/
	HANDLE hFile = file->fil_desc;

	DWORD dwFileSizeHigh;
	const DWORD dwFileSizeLow = GetFileSize(hFile, &dwFileSizeHigh);

	if ((dwFileSizeLow == INVALID_FILE_SIZE) && (GetLastError() != NO_ERROR))
		nt_error("GetFileSize", file, isc_io_access_err, 0);

    const ULONGLONG ullFileSize = (((ULONGLONG) dwFileSizeHigh) << 32) + dwFileSizeLow;
	return (ULONG) ((ullFileSize + pagesize - 1) / pagesize);
}


static jrd_file* seek_file(jrd_file*	file,
					 	   BufferDesc*	bdb,
					 	   FbStatusVector*	/*status_vector*/,
					 	   OVERLAPPED*	overlapped)
{
/**************************************
 *
 *	s e e k _ f i l e
 *
 **************************************
 *
 * Functional description
 *	Given a buffer descriptor block, find the appropriate
 *	file block and seek to the proper page in that file.
 *
 **************************************/
	BufferControl *bcb = bdb->bdb_bcb;
	ULONG page = bdb->bdb_page.getPageNum();

	for (;; file = file->fil_next)
	{
		if (!file) {
			CORRUPT(158);		// msg 158 database file not available
		}
		else if (page >= file->fil_min_page && page <= file->fil_max_page) {
			break;
		}
	}

	page -= file->fil_min_page - file->fil_fudge;

    LARGE_INTEGER liOffset;
	liOffset.QuadPart = UInt32x32To64((DWORD) page, (DWORD) bcb->bcb_page_size);

	overlapped->Offset = liOffset.LowPart;
	overlapped->OffsetHigh = liOffset.HighPart;
	overlapped->Internal = 0;
	overlapped->InternalHigh = 0;
	overlapped->hEvent = (HANDLE) 0;

	return file;
}


static jrd_file* setup_file(Database* dbb,
							const Firebird::PathName& file_name,
							HANDLE desc,
							bool read_only,
							bool shareMode)
{
/**************************************
 *
 *	s e t u p _ f i l e
 *
 **************************************
 *
 * Functional description
 *	Set up file and lock blocks for a file.
 *
 **************************************/
	jrd_file* file = NULL;

	try
	{
		file = FB_NEW_RPT(*dbb->dbb_permanent, file_name.length() + 1) jrd_file();
		file->fil_desc = desc;
		file->fil_max_page = MAX_ULONG;
		strcpy(file->fil_string, file_name.c_str());

		if (read_only)
			file->fil_flags |= FIL_readonly;
		if (shareMode)
			file->fil_flags |= FIL_sh_write;

		// If this isn't the primary file, we're done

		const PageSpace* const pageSpace = dbb->dbb_page_manager.findPageSpace(DB_PAGE_SPACE);
		if (pageSpace && pageSpace->file)
			return file;

#ifdef WIN9X_SUPPORT
		// Disable sophisticated file extension when running on 9X
		if (ISC_is_WinNT())
#endif
		{
			file->fil_ext_lock = FB_NEW_POOL(*dbb->dbb_permanent) Firebird::RWLock();
		}
	}
	catch (const Firebird::Exception&)
	{
		CloseHandle(desc);
		delete file;
		throw;
	}

	fb_assert(file);
	return file;
}

static bool maybeCloseFile(HANDLE& hFile)
{
/**************************************
 *
 *	M a y b e C l o s e F i l e
 *
 **************************************
 *
 * Functional description
 *	If the file is open, close it.
 *
 **************************************/

	if (hFile != INVALID_HANDLE_VALUE)
	{
		CloseHandle(hFile);
		hFile = INVALID_HANDLE_VALUE;
		return true;
	}
	return false;
}

static bool nt_error(const TEXT* string,
	const jrd_file* file, ISC_STATUS operation,
	FbStatusVector* const status_vector)
{
/**************************************
 *
 *	n t _ e r r o r
 *
 **************************************
 *
 * Functional description
 *	Somebody has noticed a file system error and expects error
 *	to do something about it.  Harumph!
 *
 **************************************/
	const DWORD lastError = GetLastError();

MessageBox(NULL, string, "nt_error", MB_OK);

	Arg::StatusVector status;
	status << Arg::Gds(isc_io_error) << Arg::Str(string) << Arg::Str(file->fil_string) <<
			  Arg::Gds(operation);
	if (lastError != ERROR_SUCCESS)
		status << Arg::Windows(lastError);

	if (!status_vector)
		ERR_post(status);

	ERR_build_status(status_vector, status);
	iscLogStatus(NULL, status_vector);

	return false;
}

// These are defined in Windows Server 2008 SDK
#ifndef FILE_CACHE_FLAGS_DEFINED
#define FILE_CACHE_MAX_HARD_ENABLE      0x00000001
#define FILE_CACHE_MAX_HARD_DISABLE     0x00000002
#define FILE_CACHE_MIN_HARD_ENABLE      0x00000004
#define FILE_CACHE_MIN_HARD_DISABLE     0x00000008
#endif // FILE_CACHE_FLAGS_DEFINED

BOOL SetPrivilege(
	HANDLE hToken,			// access token handle
	LPCTSTR lpszPrivilege,	// name of privilege to enable/disable
	BOOL bEnablePrivilege)	// to enable or disable privilege
{
	TOKEN_PRIVILEGES tp;
	LUID luid;

	if (!LookupPrivilegeValue(
			NULL,			// lookup privilege on local system
			lpszPrivilege,	// privilege to lookup
			&luid))			// receives LUID of privilege
	{
		// gds__log("LookupPrivilegeValue error: %u", GetLastError() );
		return FALSE;
	}

	tp.PrivilegeCount = 1;
	tp.Privileges[0].Luid = luid;
	if (bEnablePrivilege)
		tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
	else
		tp.Privileges[0].Attributes = 0;

	// Enable or disable the privilege

	if (!AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(TOKEN_PRIVILEGES),
			(PTOKEN_PRIVILEGES) NULL, (PDWORD) NULL))
	{
		//gds__log("AdjustTokenPrivileges error: %u", GetLastError() );
		return FALSE;
	}

	if (GetLastError() == ERROR_NOT_ALL_ASSIGNED)
	{
		//gds__log("The token does not have the specified privilege");
		return FALSE;
	}

	return TRUE;
}

static void adjustFileSystemCacheSize()
{
	int percent = Config::getFileSystemCacheSize();

	// firebird.conf asks to do nothing
	if (percent == 0)
		return;

	// Ensure that the setting has a sensible value
	if (percent > 95 || percent < 10)
	{
		gds__log("Incorrect FileSystemCacheSize setting %d. Using default (30 percent).", percent);
		percent = 30;
	}

	HMODULE hmodKernel32 = GetModuleHandle("kernel32.dll");

	// This one requires 64-bit XP or Windows Server 2003 SP1
	typedef BOOL (WINAPI *PFnSetSystemFileCacheSize)(SIZE_T, SIZE_T, DWORD);

	typedef BOOL (WINAPI *PFnGetSystemFileCacheSize)(PSIZE_T, PSIZE_T, PDWORD);

	// This one needs any NT, but load it dynamically anyways just in case
	typedef BOOL (WINAPI *PFnGlobalMemoryStatusEx)(LPMEMORYSTATUSEX);

	PFnSetSystemFileCacheSize pfnSetSystemFileCacheSize =
		(PFnSetSystemFileCacheSize) GetProcAddress(hmodKernel32, "SetSystemFileCacheSize");
	PFnGetSystemFileCacheSize pfnGetSystemFileCacheSize =
		(PFnGetSystemFileCacheSize) GetProcAddress(hmodKernel32, "GetSystemFileCacheSize");
	PFnGlobalMemoryStatusEx pfnGlobalMemoryStatusEx =
		(PFnGlobalMemoryStatusEx) GetProcAddress(hmodKernel32, "GlobalMemoryStatusEx");

	// If we got too old OS and functions are not there - do not bother
	if (!pfnGetSystemFileCacheSize || !pfnSetSystemFileCacheSize || !pfnGlobalMemoryStatusEx)
		return;

	MEMORYSTATUSEX msex;
	msex.dwLength = sizeof(msex);

	// This should work
	if (!pfnGlobalMemoryStatusEx(&msex))
		system_call_failed::raise("GlobalMemoryStatusEx", GetLastError());

	SIZE_T origMinimumFileCacheSize, origMaximumFileCacheSize;
	DWORD origFlags;

	BOOL result = pfnGetSystemFileCacheSize(&origMinimumFileCacheSize,
		&origMaximumFileCacheSize, &origFlags);

	if (!result)
	{
		const DWORD error = GetLastError();
#ifndef _WIN64
		// This error is returned on 64-bit Windows when the file cache size
		// overflows the ULONG limit restricted by the 32-bit Windows API.
		// Let's avoid writing it into the log as it's not a critical failure.
		if (error != ERROR_ARITHMETIC_OVERFLOW)
#endif
		gds__log("GetSystemFileCacheSize error %d", error);
		return;
	}

	// Somebody has already configured maximum cache size limit
	// Hope it is a sensible one - do not bother to adjust it
	if ((origFlags & FILE_CACHE_MAX_HARD_ENABLE) != 0)
		return;

	DWORDLONG maxMem = (msex.ullTotalPhys / 100) * percent;

#ifndef _WIN64
	// If we are trying to set the limit so high that it doesn't fit
	// in 32-bit API - leave settings alone and write a message to log file
	if (maxMem > (SIZE_T)(-2))
	{
		gds__log("Could not use 32-bit SetSystemFileCacheSize API to set cache size limit to %I64d."
				" Please use 64-bit engine or configure cache size limit externally", maxMem);
		return;
	}
#endif

	HANDLE hToken;
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken))
	{
		gds__log("OpenProcessToken error %d", GetLastError());
		return;
	}

	if (SetPrivilege(hToken, "SeIncreaseQuotaPrivilege", TRUE))
	{
		result = pfnSetSystemFileCacheSize(0, maxMem, FILE_CACHE_MAX_HARD_ENABLE);
		const DWORD error = GetLastError();
		SetPrivilege(hToken, "SeIncreaseQuotaPrivilege", FALSE);

		if (!result)
		{
			// If we do not have enough permissions - silently ignore the error
			gds__log("SetSystemFileCacheSize error %d. "
				"The engine will continue to operate, but the system "
				"performance may degrade significantly when working with "
				"large databases", error);
		}
	}

	CloseHandle(hToken);
}


/// class PIORequest


void PIORequest::markCompletion(bool error, size_t ioSize)
{
	if (error || ioSize != m_ioSize)
	{
		m_state = PIORequest::PIOR_ERROR;
		m_osError = GetLastError();
	}
	else 
	{
		m_state = PIORequest::PIOR_COMPLETED;
		m_osError = 0;
	}

	if (m_file->fil_ext_lock)
		m_file->fil_ext_lock->endRead();
}


/// class PIOPort

PIOPort::PIOPort()
{
	m_handle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
}

PIOPort::~PIOPort()
{
	CloseHandle(m_handle);
}

void PIOPort::addFile(jrd_file* file)
{
	if (!(file->fil_flags & FIL_aio_init))
	{
		SyncLockGuard sync(&m_sync, SYNC_EXCLUSIVE, "PIOPort::addFile");

		if (file->fil_flags & FIL_aio_init)
			return;

		if (CreateIoCompletionPort(file->fil_desc, m_handle, (PIO_EVENT_T) file, 0) != m_handle)
			DWORD err = GetLastError();
		else
			file->fil_flags |= FIL_aio_init;
	}
}

void PIOPort::removeFile(jrd_file* file)
{
	SyncLockGuard sync(&m_sync, SYNC_EXCLUSIVE, "PIOPort::removeFile");

	if (file->fil_flags & FIL_aio_init)
		file->fil_flags &= ~FIL_aio_init;
}


PIO_EVENT_T PIOPort::getCompletedRequest(thread_db* tdbb, PIORequest** pio, int timeout)
{
	*pio = NULL;

	DWORD dwSize = 0;
	PIO_EVENT_T pKey = 0;
	OVERLAPPED* pOvrl = NULL;
	DWORD ret = 0;
	{
		EngineCheckout cout(tdbb, FB_FUNCTION);
		ret = GetQueuedCompletionStatus(m_handle, &dwSize, &pKey, &pOvrl, timeout);
	}

	if (pOvrl && pKey != PIO_EVENT_WAKEUP)
	{
		PIORequest* req = PIORequest::fromOSData(pOvrl);
		req->markCompletion(ret == 0, dwSize);
	
		*pio = req;
		pKey = PIO_EVENT_IO;
	}
	else if (!pOvrl && !ret && GetLastError() == WAIT_TIMEOUT)
		pKey = PIO_EVENT_TIMEOUT;

	return pKey;
}

void PIOPort::postEvent(PIO_EVENT_T pioEvent)
{
	if (!PostQueuedCompletionStatus(m_handle, 0, pioEvent, NULL))
		DWORD err = GetLastError();
}
