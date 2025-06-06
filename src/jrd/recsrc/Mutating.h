#ifndef JRD_MUTATING_H
#define JRD_MUTATING_H

#include "../../common/config/config.h"
#include "../../jrd/exe.h"
#include "../../jrd/jrd.h"
#include "../../jrd/tra.h"
#include "../../dsql/StmtNodes.h"
#include "Cursor.h"


namespace Jrd
{

// Used before start of read or change relation(s) to check if it(s) currently changing (mutating)
class MutationCheck
{
public:
	// if any relation of Select is mutating, throw error or put warning and returns true
	// else returns false
	static void checkSelect(thread_db* tdbb, const Request* request, const Select* select);

	// if relation is mutating, throw error or put warning and returns true
	// else returns false
	static bool checkRelation(thread_db* tdbb, const Request* request, const jrd_rel* relation);

	// returns true if statement is subject of mutation checks
	static bool needStatement(const Statement* stmt);

	// returns true if trigger is not NULL and it is FK trigger and relId is the trigger's relation
	static bool isSelfFK(const Trigger* trigger, USHORT relId);

private:
	// returns true if further checks are required
	static bool preCheck(thread_db* tdbb, const Request* request);

	// if relation is mutating, throw error or put warning and returns true
	// else returns false
	static bool relMutating(thread_db* tdbb, const Request* request, const jrd_rel* relation);
};

// Used to mark relation as mutating.
// Before storing of first record, table is not mutated yet, and pre-store
// triggers can't overwrite any changes. Thus mutating mark is set after
// pre-store triggers, not before them.
//
// In the case of modify and erase, mark is set before run of pre- triggers.
// For the multy-record operations, mark is removed by ForNode after last record
// processed. For the single-record operations there is no ForNode and mark is
// removed after run of post- triggers.
class MutationMark
{
public:
	MutationMark(thread_db* tdbb, Request* request, const jrd_rel* relation, const ForNode* forNode)
	{
		if (!tdbb->getDatabase()->dbb_config->getCheckMutatingTables())
			return;

		if (!relation || !relation->isMutable())
			return;

		if (!MutationCheck::needStatement(request->getStatement()))
			return;

		m_relId = relation->rel_id;

		// In case of self-ref FK trigger, relation is already mutating
		// and should not be marked again
		if (MutationCheck::isSelfFK(request->getStatement()->trigger, m_relId))
			return;

		if (forNode)
		{
			forNode->setMutating(tdbb, request, m_relId);
		}
		else
		{
			m_transaction = tdbb->getTransaction();
			m_transaction->setMutating(m_relId, true);
		}
	}

	~MutationMark()
	{
		release();
	}

	void release()
	{
		if (m_transaction)
		{
			m_transaction->setMutating(m_relId, false);
			m_transaction = nullptr;
		}
	}

private:
	jrd_tra* m_transaction = nullptr;
	USHORT m_relId = 0;
};

}; //namespace Jrd

#endif // JRD_MUTATING_H
