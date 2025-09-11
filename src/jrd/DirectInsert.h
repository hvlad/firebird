#ifndef JRD_DIRECTINSERT
#define JRD_DIRECTINSERT

#include "firebird.h"
#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../jrd/jrd.h"
#include "../jrd/ods.h"

namespace Jrd
{

class jrd_rel;
class jrd_tra;
struct record_param;
class thread_db;

class DirectInsert
{
public:
	DirectInsert(Firebird::MemoryPool& pool, jrd_rel* relation, ULONG pageSize);

	void putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction);

	void flush(thread_db* tdbb);

private:
	// allocate and reserve data pages
	Ods::data_page* allocatePages(thread_db* tdbb);
	UCHAR* findSpace(thread_db* tdbb, record_param* rpb, USHORT size);


	Firebird::MemoryPool& m_pool;
	jrd_rel* const m_relation;
	const ULONG m_pageSize;
	win m_window;								// current data page, locked for write
	Ods::data_page* m_current = nullptr;		// current DP to put records
	ULONG m_freeSpace = 0;						// free space on current DP
	ULONG m_reserved = 0;						// count of reserved pages
	ULONG m_lastReserved = 0;					// number of last reserved page
	USHORT m_firstSlot = 0;						// slot number of the first	reserved page
};

};	// namespace Jrd

#endif // JRD_DIRECTINSERT
