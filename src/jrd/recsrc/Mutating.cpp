
#include "../../common/classes/fb_string.h"
#include "../../jrd/err_proto.h"
#include "../../jrd/exe_proto.h"
#include "Mutating.h"

using namespace Firebird;

namespace Jrd {

bool MutationCheck::preCheck(thread_db* tdbb, const Request* request)
{
	const int errMode = tdbb->getDatabase()->dbb_config->getCheckMutatingTables();
	if (errMode == 0)
		return false;

	if (!needStatement(request->getStatement()))
		return false;

	if (!tdbb->getTransaction()->hasMutating())
		return false;

	return true;
}

void MutationCheck::checkSelect(thread_db* tdbb, const Request* request, const Select* select)
{
	if (!preCheck(tdbb, request) || !select)
		return;

	const auto* stmt = request->getStatement();

	StreamList streams;
	select->getRootRecordSource()->findUsedStreams(streams, true);

	const Trigger* dmlTrigger = stmt->trigger;
	if (dmlTrigger && (dmlTrigger->trigType & TRIGGER_TYPE_MASK) != TRIGGER_TYPE_DML)
		dmlTrigger = nullptr;

	for (auto stream : streams)
	{
		// Skip OLD and NEW contexts
		if (dmlTrigger && (stream < 2))
			continue;

		const jrd_rel* relation = request->req_rpb[stream].rpb_relation;
		if (!relation || !relation->isMutable())
			continue;

		// self-ref system triggers is allowed
		if (isSelfFK(dmlTrigger, relation->rel_id))
			continue;

		if (relMutating(tdbb, request, relation))
			break;  // should we report more than a single relation ?
	}
}

bool MutationCheck::checkRelation(thread_db* tdbb, const Request* request, const jrd_rel* relation)
{
	if (!relation || !relation->isMutable() || !preCheck(tdbb, request))
		return false;

	if (isSelfFK(request->getStatement()->trigger, relation->rel_id))
		return false;

	return relMutating(tdbb, request, relation);
}

bool MutationCheck::relMutating(thread_db* tdbb, const Request* request, const jrd_rel* relation)
{
	fb_assert(relation);

	const jrd_tra* transaction = tdbb->getTransaction();
	if (!transaction->checkMutating(relation->rel_id))
		return true;

	const int errMode = tdbb->getDatabase()->dbb_config->getCheckMutatingTables();
	string msg;
	msg.printf("Access of mutating table ''%s''", relation->rel_name.c_str());

	if (errMode == 2)
		ERR_post(Arg::Gds(isc_random) << msg);

	ERR_post_warning(Arg::Warning(isc_random) << msg);

	string stack;
	if (EXE_get_stack_trace(request, stack))
		ERR_post_warning(Arg::Warning(isc_stack_trace) << stack);

	return false;
}

bool MutationCheck::needStatement(const Statement* stmt)
{
	// return (!(stmt->flags & Statement::FLAG_INTERNAL) || (stmt->flags & Statement::FLAG_SYS_TRIGGER));

	// User statements must be checked
	if (!(stmt->flags & Statement::FLAG_INTERNAL))
		return true;

	// Internal statements are not checked, unless it is an constraint trigger
	const auto* trg = stmt->trigger;
	if (!trg)
		return false;

	switch (trg->sysTrigger)
	{
	case fb_sysflag_check_constraint:
	case fb_sysflag_referential_constraint:
		return true;
	default:
		return false;
	}

	return false;
}

bool MutationCheck::isSelfFK(const Trigger* trg, USHORT relId)
{
	return (trg &&
		(trg->sysTrigger == fb_sysflag_referential_constraint) &&
		(trg->relation->rel_id == relId));
}


};	// namespace Jrd
