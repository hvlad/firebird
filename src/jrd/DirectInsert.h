#ifndef JRD_DIRECTINSERT
#define JRD_DIRECTINSERT

#include "firebird.h"
#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
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
	// allocate count pages, init page headers in buffer, pointer by ptr
	void allocatePages(thread_db* tdbb, unsigned count, UCHAR* ptr);
	UCHAR* findSpace(thread_db* tdbb, record_param* rpb, USHORT size);


	Firebird::MemoryPool& m_pool;
	jrd_rel* m_relation;
	const ULONG m_pageSize;

	Firebird::Array<UCHAR> m_primary;		// buffer for pages with primary records
	Firebird::Array<UCHAR> m_fragments;		// buffer for pages with fragments
	UCHAR* m_dp;							// pointer to the first primary DP
	UCHAR* m_frgm;							// pointer to the first fragments DP
	Ods::data_page* m_current;				// current DP to put records
	ULONG m_freeSpace;						// free space on current DP
};

};	// namespace Jrd

#endif // JRD_DIRECTINSERT
