#ifndef JRD_BULKINSERT
#define JRD_BULKINSERT

#include "firebird.h"
#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../jrd/jrd.h"
#include "../jrd/ods.h"

namespace Jrd
{

class Compressor;
class jrd_rel;
class jrd_tra;
struct record_param;
class Request;

class BulkInsert
{
public:
	BulkInsert(Firebird::MemoryPool& pool, thread_db* tdbb, jrd_rel* relation);

	void putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction);

	void flush(thread_db* tdbb);

	Request* getRequest() const
	{
		return m_request;
	}

	jrd_rel* getRelation() const
	{
		return m_relation;
	}

private:
	// allocate and reserve data pages
	Ods::data_page* allocatePages(thread_db* tdbb);
	UCHAR* findSpace(thread_db* tdbb, record_param* rpb, USHORT size);
	void fragmentRecord(thread_db* tdbb, record_param* rpb, Compressor* dcc);


	Firebird::MemoryPool& m_pool;
	jrd_rel* const m_relation;
	Request* const m_request;
	const ULONG m_pageSize;
	const ULONG m_spaceReserve;
	win m_window;								// current data page, locked for write
	Ods::data_page* m_current = nullptr;		// current DP to put records
	ULONG m_freeSpace = 0;						// free space on current DP
	ULONG m_reserved = 0;						// count of reserved pages
	ULONG m_lastReserved = 0;					// number of last reserved page
	USHORT m_firstSlot = 0;						// slot number of the first	reserved page
	ULONG m_largeMask = 0;						// bitmask of reserved pages with large objects
};

};	// namespace Jrd

#endif // JRD_BULKINSERT
