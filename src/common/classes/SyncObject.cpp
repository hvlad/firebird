/*
 *
 *     The contents of this file are subject to the Initial
 *     Developer's Public License Version 1.0 (the "License");
 *     you may not use this file except in compliance with the
 *     License. You may obtain a copy of the License at
 *     http://www.ibphoenix.com/idpl.html.
 *
 *     Software distributed under the License is distributed on
 *     an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either
 *     express or implied.  See the License for the specific
 *     language governing rights and limitations under the License.
 *
 *     The contents of this file or any work derived from this file
 *     may not be distributed under any other license whatsoever
 *     without the express prior written permission of the original
 *     author.
 *
 *
 *  The Original Code was created by James A. Starkey for IBPhoenix.
 *
 *  Copyright (c) 1997 - 2000, 2001, 2003 James A. Starkey
 *  Copyright (c) 1997 - 2000, 2001, 2003 Netfrastructure, Inc.
 *  All Rights Reserved.
 *
 *  The Code was ported into Firebird Open Source RDBMS project by
 *  Vladyslav Khorsun at 2010
 *
 *  Contributor(s):
 */

#include "firebird.h"
#include "../common/gdsassert.h"
#include "fb_exception.h"

#include "SyncObject.h"
#include "Synchronize.h"

using namespace std;

namespace Firebird {

bool SyncObject::tryLockShared(wait_type w, const char* from)
{
	//while (waiters.load(memory_order_acquire) == w)
	//while ((waiters.load(memory_order_acquire) & WAIT_WRITERS_MASK) == 0)
	while (true)
	{
		if (lockState.load(memory_order_seq_cst) < 0)
			break;

		if (lockState.fetch_add(STATE_READER_INCR) >= 0)
		{
#ifdef DEV_BUILD
			MutexLockGuard g(mutex, FB_FUNCTION);
			reason(from);
#endif
			return true;
		}

		// fix mistake
		if (lockState.fetch_add(-STATE_READER_INCR) == STATE_READER_INCR)
			grantLocks();
		else
#ifdef WIN_NT
			YieldProcessor();
#else
			Thread::yield();
#endif
	}

	return false;
}

bool SyncObject::tryLockExclusuve(wait_type w, ThreadSync* thread, const char* from)
{
	//while (waiters.load(memory_order_acquire) == w)
	while (true)
	{
		const wait_type val = waiters.load();
		if ((val & WAIT_WRITERS_MASK) != w || (val & WAIT_READERS_MASK) != 0)
			break;

		if (lockState.load() != 0)
			break;

		if (lockState.fetch_add(STATE_WRITER_INCR) == 0)
		{
			exclusiveThread.store(thread, memory_order_relaxed);
#ifdef DEV_BUILD
			MutexLockGuard g(mutex, FB_FUNCTION);
			reason(from);
#endif
			return true;
		}

		// fix mistake
		if (lockState.fetch_add(-STATE_WRITER_INCR) == STATE_WRITER_INCR)
			grantLocks();
		else
#ifdef WIN_NT
			YieldProcessor();
#else
			Thread::yield();
#endif
	}

	return false;
}

bool SyncObject::lock(Sync* sync, SyncType type, const char* from, int timeOut)
{
	ThreadSync* thread = NULL;

	if (type == SYNC_SHARED)
	{
		if (tryLockShared(0, from))
			return true;

		if (timeOut == 0)
			return false;

		mutex.enter(FB_FUNCTION);
		waiters.fetch_add(WAIT_READER_INCR, memory_order_release);

		if (tryLockShared(WAIT_READER_INCR, from))
		{
			waiters.fetch_sub(WAIT_READER_INCR, memory_order_release);
			mutex.leave();
			return true;
		}

		thread = ThreadSync::findThread();
		fb_assert(thread);
	}
	else
	{
		fb_assert(type == SYNC_EXCLUSIVE);

		thread = ThreadSync::findThread();
		fb_assert(thread);

		if (thread == exclusiveThread.load(memory_order_relaxed))
		{
			++monitorCount;
#ifdef DEV_BUILD
			MutexLockGuard g(mutex, FB_FUNCTION);
			reason(from);
#endif
			return true;
		}

		if (tryLockExclusuve(0, thread, from))
			return true;

		if (timeOut == 0)
			return false;

		mutex.enter(FB_FUNCTION);
		waiters.fetch_add(WAIT_WRITER_INCR, memory_order_release);

		if (tryLockExclusuve(WAIT_WRITER_INCR, thread, from))
		{
			waiters.fetch_sub(WAIT_WRITER_INCR, memory_order_release);
			mutex.leave();
			return true;
		}
	}

	return wait(type, thread, sync, timeOut);
}

bool SyncObject::lockConditional(SyncType type, const char* from)
{
	if (type == SYNC_SHARED)
		return tryLockShared(0, from);

	fb_assert(type == SYNC_EXCLUSIVE);

	ThreadSync* thread = ThreadSync::findThread();
	fb_assert(thread);

	if (thread == exclusiveThread.load(memory_order_relaxed))
	{
		++monitorCount;
#ifdef DEV_BUILD
		MutexLockGuard g(mutex, FB_FUNCTION);
		reason(from);
#endif
		return true;
	}
	return tryLockExclusuve(0, thread, from);
}

void SyncObject::unlock(Sync* /*sync*/, SyncType type)
{
	//fb_assert((type == SYNC_SHARED && lockState > 0) ||
	//	(type == SYNC_EXCLUSIVE && lockState == STATE_WRITER_INCR));

	if (monitorCount)
	{
		fb_assert(monitorCount > 0);
		--monitorCount;
		return;
	}

	exclusiveThread.store(NULL, memory_order_relaxed);
	const state_type incr = (type == SYNC_SHARED) ? STATE_READER_INCR : STATE_WRITER_INCR;

	if (lockState.fetch_add(-incr) == incr)
		//if (waiters.load())
			grantLocks();
}

void SyncObject::downgrade(SyncType type)
{
	fb_assert(monitorCount == 0);
	fb_assert(type == SYNC_SHARED);
	fb_assert(lockState == STATE_WRITER_INCR);
	fb_assert(exclusiveThread.load() != NULL);
	fb_assert(exclusiveThread == ThreadSync::findThread());

	exclusiveThread.store(NULL, memory_order_relaxed);

	if (lockState.fetch_add(STATE_READER_INCR - STATE_WRITER_INCR) == STATE_WRITER_INCR)
		if (waiters.load() & WAIT_READERS_MASK)
			grantLocks();
}

bool SyncObject::wait(SyncType type, ThreadSync* thread, Sync* sync, int timeOut)
{
	if (thread->nextWaiting)
	{
		mutex.leave();
		fatal_exception::raise("single thread deadlock");
	}

	ThreadSync* head = waitingThreads;
	if (head)
	{
		thread->prevWaiting = head->prevWaiting;
		thread->nextWaiting = head;

		head->prevWaiting->nextWaiting = thread;
		head->prevWaiting = thread;
	}
	else
	{
		thread->prevWaiting = thread->nextWaiting = thread;
		waitingThreads = thread;
	}

	thread->lockType = type;
	thread->lockGranted = false;
	thread->lockPending = sync;
	mutex.leave();

	bool wakeup = false;
	while (timeOut && !thread->lockGranted)
	{
		const int wait = timeOut > 10000 ? 10000 : timeOut;
		wakeup = true;

		static int stallMs = 500;
		if (timeOut == -1)
			wakeup = thread->sleep(stallMs);
		else
			wakeup = thread->sleep(wait);

		if (thread->lockGranted)
			return true;

		if (!wakeup)
			stalled(thread);

		if (timeOut != -1)
			timeOut -= wait;
	}

	if (thread->lockGranted)
		return true;

	MutexLockGuard guard(mutex, "SyncObject::wait");
	if (thread->lockGranted)
		return true;

	dequeThread(thread);
	if (type == SYNC_SHARED)
		waiters.fetch_sub(WAIT_READER_INCR, memory_order_release);
	else
		waiters.fetch_sub(WAIT_WRITER_INCR, memory_order_release);

	fb_assert(timeOut >= 0);
	return false;
}

void SyncObject::stalled(ThreadSync* thread)
{
	//static int cnt = 0;
	//cnt++;
}

ThreadSync* SyncObject::grantThread(ThreadSync* thread)
{
	ThreadSync* next = dequeThread(thread);
	thread->grantLock(this);
	return next;
}

ThreadSync* SyncObject::dequeThread(ThreadSync* thread)
{
	ThreadSync* next = NULL;

	if (thread == thread->nextWaiting)
	{
		thread->nextWaiting = thread->prevWaiting = NULL;
		waitingThreads = NULL;
	}
	else
	{
		next = thread->nextWaiting;

		thread->prevWaiting->nextWaiting = next;
		next->prevWaiting = thread->prevWaiting;

		thread->nextWaiting = thread->prevWaiting = NULL;
		if (waitingThreads == thread)
			waitingThreads = next;
	}

	return next;
}

void SyncObject::grantLocks()
{
	if (waiters.load() == 0)
		return;

	MutexLockGuard guard(mutex, "SyncObject::grantLocks");

	ThreadSync* thread = waitingThreads;
	fb_assert((waiters && thread) || (!waiters && !thread));

	if (!thread)
		return;

	while (true)
	{
		if (thread->lockType == SYNC_SHARED)
		{
			const wait_type cntWake = (waiters.load() & WAIT_READERS_MASK);
			const state_type stateInc = cntWake * STATE_READER_INCR;
			if (lockState.fetch_add(stateInc, memory_order_release) >= 0)
			{
				waiters.fetch_sub(cntWake, memory_order_release);

				for (wait_type i = 0; i < cntWake;)
				{
					if (thread->lockType == SYNC_SHARED)
					{
						ThreadSync* next = dequeThread(thread);
						thread->grantLock(this);
						thread = next;
						i++;
					}
					else
					{
						thread = thread->nextWaiting;
					}
				}
			}
			else if (lockState.fetch_add(-stateInc, memory_order_release) == stateInc)
				continue;
		}
		else
		{
			if (lockState.load() > 0)
				break;

			if (lockState.fetch_add(STATE_WRITER_INCR, memory_order_release) == 0)
			{
				exclusiveThread.store(thread, memory_order_relaxed);
				waiters.fetch_sub(WAIT_WRITER_INCR, memory_order_release);
				dequeThread(thread);
				thread->grantLock(this);
			}
			else if (lockState.fetch_add(-STATE_WRITER_INCR, memory_order_release) == STATE_WRITER_INCR)
				continue;
		}
		break;
	}
}

void SyncObject::validate(SyncType lockType) const
{
	switch (lockType)
	{
	case SYNC_NONE:
		fb_assert(lockState == 0);
		break;

	case SYNC_SHARED:
		fb_assert(lockState > 0);
		break;

	case SYNC_EXCLUSIVE:
		fb_assert(lockState == STATE_WRITER_INCR);
		break;
	}
}

bool SyncObject::ourExclusiveLock() const
{
	if (lockState > 0)
		return false;

	return (exclusiveThread.load(memory_order_seq_cst) == ThreadSync::findThread());
}

} // namespace Firebird
