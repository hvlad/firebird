#include <firebird.h>
#include "../jrd/exe.h"
#include "../jrd/jrd.h"
#include "../jrd/obj.h"
#include "../jrd/Function.h"
#include "../jrd/Statement.h"
#include "../jrd/recsrc/RecordSource.h"
#include "../jrd/recsrc/Cursor.h"
#include "../jrd/met_proto.h"
#include "../jrd/QualifiedName.h"

using namespace Firebird;

namespace Jrd
{

class DepsWalker
{
public:
	DepsWalker()	:
		m_path(*getDefaultMemoryPool()),
		m_pathIndex(*getDefaultMemoryPool()),
		m_walked(*getDefaultMemoryPool())
	{}

	string walkRelation(thread_db* tdbb, jrd_rel* relation, bool all);

private:

	enum Action
	{
		ACT_none = 0,
		ACT_insert,
		ACT_update,
		ACT_delete,
		ACT_select,
		ACT_execute
	};

	static Action getAction(ExternalAccess::exa_act ex_act)
	{
		switch (ex_act)
		{
		case ExternalAccess::exa_insert:
			return ACT_insert;
		case ExternalAccess::exa_update:
			return ACT_update;
		case ExternalAccess::exa_delete:
			return ACT_delete;
		default:
			return ACT_none;
		}
	}

	struct PathItem
	{
		PathItem() :
			m_type(0),
			m_id(0),
			m_action(ACT_none)
		{}

		PathItem(const jrd_rel* relation, Action action) :
			m_name(relation->rel_name),
			m_type(obj_relation),
			m_id(relation->rel_id),
			m_action(action)
		{}


		PathItem(const jrd_rel* relation, ExternalAccess::exa_act action) :
			m_name(relation->rel_name),
			m_type(obj_relation),
			m_id(relation->rel_id),
			m_action(getAction(action))
		{}

		PathItem(jrd_prc* procedure) :
			m_name(procedure->getName()),
			m_type(obj_procedure),
			m_id(procedure->getId()),
			m_action(ACT_execute)
		{}

		PathItem(Function* function) :
			m_name(function->getName()),
			m_type(obj_udf),
			m_id(function->getId()),
			m_action(ACT_execute)
		{}

		PathItem(Trigger* trigger, Action action) :
			m_name(trigger->name),
			m_type(obj_trigger),
			m_id(0),
			m_sysFlag(trigger->sysTrigger),
			m_action(action)
		{}

		string print() const;

		QualifiedName m_name;
		ObjectType m_type;
		USHORT m_id;
		SSHORT m_sysFlag = fb_sysflag_user;
		Action m_action;

		bool operator>(const PathItem& other) const
		{
			if (m_type > other.m_type)
				return true;

			if (m_type == other.m_type)
			{
				if (m_type == obj_trigger)
					return m_name > other.m_name;

				return (m_id > other.m_id);
			}

			return false;
		}

		bool operator==(const PathItem& other) const
		{
			if (m_type != other.m_type)
				return false;

			if (m_type == obj_trigger)
				return m_name == other.m_name;

			return (m_id == other.m_id);
		}
	};

	class PathStep
	{
	public:
		PathStep(DepsWalker* parent, const PathItem& item)
		{
			m_parent = parent;

			FB_SIZE_T pos;
			m_exists = m_parent->m_pathIndex.find(item, pos);
			if (!m_exists)
			{
				m_parent->m_pathIndex.insert(pos, item);
				m_parent->m_path.push(item);
			}
		}

		~PathStep()
		{
			if (!m_exists)
			{
				const PathItem item = m_parent->m_path.pop();

				FB_SIZE_T pos;
				if (m_parent->m_pathIndex.find(item, pos))
					m_parent->m_pathIndex.remove(pos);
			}
		}

		bool exists() const { return m_exists;  }

	private:
		bool m_exists;
		DepsWalker* m_parent;
	};

	void walkTriggers(thread_db* tdbb, TrigVector* vectors[], int count, Action action);
	void walkStatement(thread_db* tdbb, const Statement* stmt);
	bool walked(const PathItem& item);
	void found(const jrd_rel* relation, const PathItem& relItem);
	void printPath(const PathItem& foundItem);

	jrd_rel* m_relation = nullptr;		// relation to check
	Array<PathItem> m_path;				// current path, plain stack
	SortedArray<PathItem> m_pathIndex;	// current path, indexed by item
	SortedArray<PathItem> m_walked;		// already walked routines and triggers
	string m_result;					// result string
	bool m_all;							// print all found items, or just about the m_relation
};

class RelExistsGuard
{
public:
	RelExistsGuard(thread_db* tdbb, jrd_rel* relation) :
		m_tdbb(tdbb),
		m_relation(relation)
	{
		MET_post_existence(tdbb, m_relation);
	}

	~RelExistsGuard()
	{
		MET_release_existence(m_tdbb, m_relation);
	}

private:
	thread_db* m_tdbb;
	jrd_rel* m_relation;
};

// Start of recursive walk
string DepsWalker::walkRelation(thread_db* tdbb, jrd_rel* relation, bool all)
{
	m_relation = relation;
	m_path.clear();
	m_pathIndex.clear();
	m_result.clear();
	m_all = all;

	MET_scan_relation(tdbb, m_relation);
	RelExistsGuard relGuard(tdbb, m_relation);

	{
		TrigVector* trigs[] = {m_relation->rel_pre_store, m_relation->rel_post_store};
		PathStep step(this, PathItem(m_relation, ACT_insert));
		walkTriggers(tdbb, trigs, FB_NELEM(trigs), ACT_execute);
	}

	{
		TrigVector* trigs[] = {m_relation->rel_pre_modify, m_relation->rel_post_modify};
		PathStep step(this, PathItem(m_relation, ACT_update));
		walkTriggers(tdbb, trigs, FB_NELEM(trigs), ACT_execute);
	}

	{
		TrigVector* trigs[] = {m_relation->rel_pre_erase, m_relation->rel_post_erase};
		PathStep step(this, PathItem(m_relation, ACT_delete));
		walkTriggers(tdbb, trigs, FB_NELEM(trigs), ACT_execute);
	}

	return m_result;
}

void DepsWalker::walkTriggers(thread_db* tdbb, TrigVector* vectors[], int count, Action action)
{
	// collect unique triggers
	SortedArray<Trigger*, InlineStorage<Trigger*, 16>> triggers;

	for (auto ptr = vectors; ptr < vectors + count; ptr++)
	{
		if (!*ptr)
			continue;

		for (auto& t : **ptr)
		{
			if (t.extTrigger /*|| t.sysTrigger*/)
				continue;

			FB_SIZE_T pos;
			if (!triggers.find(&t, pos))
				triggers.insert(pos, &t);
		}
	}

	for (auto trig : triggers)
	{
		trig->compile(tdbb);

		const PathItem item(trig, action);

		if (!walked(item))
		{
			PathStep step(this, item);
			if (!step.exists())
				walkStatement(tdbb, trig->statement);
		}
	}
}

void DepsWalker::walkStatement(thread_db* tdbb, const Statement* stmt)
{
	for (const ExternalAccess& item : stmt->externalList)
	{
		if (item.exa_action == ExternalAccess::exa_procedure)
		{
			jrd_prc* const procedure = MET_lookup_procedure_id(tdbb, item.exa_prc_id, false, false, 0);
			if (procedure && procedure->getStatement())
			{
				const PathItem item(procedure);
				if (walked(item))
					continue;

				PathStep step(this, item);
				if (step.exists())
					continue;

				walkStatement(tdbb, procedure->getStatement());
			}
		}
		else if (item.exa_action == ExternalAccess::exa_function)
		{
			Function* const function = Function::lookup(tdbb, item.exa_fun_id, false, false, 0);
			if (function && function->getStatement())
			{
				const PathItem item(function);
				if (walked(item))
					continue;

				PathStep step(this, item);
				if (step.exists())
					continue;

				walkStatement(tdbb, function->getStatement());
			}
		}
		else
		{
			jrd_rel* relation = MET_lookup_relation_id(tdbb, item.exa_rel_id, false);

			if (!relation)
				continue;

			MET_scan_relation(tdbb, relation);

			PathItem relItem(relation, item.exa_action);
			PathStep step(this, relItem);
			if (step.exists())
			{
				if (m_all || relation->rel_id == m_relation->rel_id)
					found(relation, relItem);
				continue;
			}

			RelExistsGuard relGuard(tdbb, m_relation);
			TrigVector* trigs[2];

			switch (item.exa_action)
			{
			case ExternalAccess::exa_insert:
				trigs[0] = relation->rel_pre_store;
				trigs[1] = relation->rel_post_store;
				break;
			case ExternalAccess::exa_update:
				trigs[0] = relation->rel_pre_modify;
				trigs[1] = relation->rel_post_modify;
				break;
			case ExternalAccess::exa_delete:
				trigs[0] = relation->rel_pre_erase;
				trigs[1] = relation->rel_post_erase;
				break;
			default:
				fb_assert(false);
				continue;
			}

			walkTriggers(tdbb, trigs, 2, getAction(item.exa_action));
		}
	}

	// Collect SELECT'ed relations

	SortedArray<USHORT, InlineStorage<USHORT, 16>> relIds;
	for (auto select : stmt->fors)
	{
		auto recSrc = select->getRootRecordSource();

		StreamList streams;
		recSrc->findUsedStreams(streams, true);

		for (auto stream : streams)
		{
			if (stmt->rpbsSetup[stream].rpb_relation)
			{
				USHORT relId = stmt->rpbsSetup[stream].rpb_relation->rel_id;

				if (m_all || relId == m_relation->rel_id)
				{
					FB_SIZE_T pos;
					if (!relIds.find(relId, pos))
						relIds.insert(pos, relId);
				}
			}
		}
	}

	// Check for SELECT access to mutating relations
	for (USHORT relId : relIds)
	{
		const jrd_rel* relation = MET_lookup_relation_id(tdbb, relId, false);

		if (!relation)
			continue;

		PathItem relItem(relation, ACT_select);
		PathStep step(this, relItem);
		if (step.exists())
		{
			found(relation, relItem);
			continue;
		}
	}
}

bool DepsWalker::walked(const PathItem& item)
{
	if (item.m_type == obj_relation)
		return false;

	FB_SIZE_T pos;
	if (m_walked.find(item, pos))
		return true;

	m_walked.insert(pos, item);
	return false;
}

void DepsWalker::found(const jrd_rel* relation, const PathItem& relItem)
{
	m_result.append("Found mutating relation '");
	m_result.append(relation->rel_name.c_str());
	m_result.append("'\n");
	printPath(relItem);
}

void DepsWalker::printPath(const PathItem& foundItem)
{
	for (const auto& item : m_path)
	{
		m_result.append((item == foundItem) ? "->" : "  ");
		m_result.append(item.print());
		m_result.append("\n");
	}

	m_result.append("->");
	m_result.append(foundItem.print());
	m_result.append("\n\n");
}

string DepsWalker::PathItem::print()	const
{
	string str;

	// set for triggers only, so far
	switch (m_sysFlag)
	{
	case fb_sysflag_user:
		break;
	case fb_sysflag_check_constraint:
		str = "CHK ";
		break;
	case fb_sysflag_referential_constraint:
		str = "REF ";
		break;
	case fb_sysflag_system:
	default:
		str = "SYS ";
		break;
	}

	switch (m_type)
	{
	case obj_procedure:
		fb_assert(m_action == ACT_execute);
		str += "PROCEDURE '";
		str += m_name.toString();
		break;
	case obj_udf:
		fb_assert(m_action == ACT_execute);
		str += "FUNCTION '";
		str += m_name.toString();
		break;
	case obj_trigger:
		str += "TRIGGER '";
		str += m_name.toString();
		break;
	case obj_relation:
		switch (m_action)
		{
		case Jrd::DepsWalker::ACT_insert:
			str = "INSERT '";
			break;
		case Jrd::DepsWalker::ACT_update:
			str = "UPDATE '";
			break;
		case Jrd::DepsWalker::ACT_delete:
			str = "DELETE '";
			break;
		case Jrd::DepsWalker::ACT_select:
			str = "SELECT '";
			break;
		default:
			fb_assert(false);
			break;
		}
		str += m_name.toString();
		break;
	default:
		fb_assert(false);
		str.printf("UNKNOWN object type %d, id %d", m_type, m_id);
		str += m_name.toString();
		return str;
	}

	str += "'";
	return str;
}



string WalkRelationDeps(thread_db* tdbb, jrd_rel* relation, bool all)
{
	DepsWalker walker;
	return walker.walkRelation(tdbb, relation, all);
};

}
