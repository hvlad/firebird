#ifndef JRD_BULKINSERT
#define JRD_BULKINSERT

#include "firebird.h"
#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../jrd/jrd.h"
#include "../jrd/ods.h"
#include "../jrd/RecordNumber.h"


namespace Jrd
{

class Compressor;
class jrd_rel;
class jrd_tra;
struct record_param;
class Record;
class Request;

class BulkInsert : public Firebird::PermanentStorage
{
public:
	BulkInsert(Firebird::MemoryPool& pool, thread_db* tdbb, jrd_rel* relation);

	void putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction);
	RecordNumber putBlob(thread_db* tdbb, blb* blob, Record* record);
	void flush(thread_db* tdbb);

	Request* getRequest() const
	{
		return m_request;
	}

	jrd_rel* getRelation() const
	{
		return m_primary->m_relation;
	}

private:
	struct Buffer : public Firebird::PermanentStorage
	{
		Buffer(Firebird::MemoryPool& pool, ULONG pageSize, ULONG m_spaceReserve, bool primary,
			jrd_rel* relation);

		void putRecord(thread_db* tdbb, record_param* rpb, jrd_tra* transaction);
		RecordNumber putBlob(thread_db* tdbb, blb* blob, Record* record);
		void flush(thread_db* tdbb);

		// allocate and reserve data pages
		Ods::data_page* allocatePages(thread_db* tdbb);
		UCHAR* findSpace(thread_db* tdbb, record_param* rpb, USHORT size);
		void fragmentRecord(thread_db* tdbb, record_param* rpb, Compressor* dcc);
		void markLarge();

		const ULONG m_pageSize;
		const ULONG m_spaceReserve;
		const bool m_isPrimary;
		jrd_rel* const m_relation;

		Firebird::Array<UCHAR> m_buffer;			// buffer for data pages
		Ods::data_page* m_pages = nullptr;			// first DP in buffer
		Ods::data_page* m_current = nullptr;		// current DP to put records
		PageStack m_highPages;						// high precedence pages	   (todo: per page)
		ULONG m_freeSpace = 0;						// free space on current DP
		USHORT m_reserved = 0;						// count of reserved pages
		USHORT m_firstSlot = 0;						// slot number of the first	reserved page
	};

	Request* const m_request;

	Firebird::AutoPtr<Buffer> m_primary;
	Firebird::AutoPtr<Buffer> m_other;
};

};	// namespace Jrd

#endif // JRD_BULKINSERT
