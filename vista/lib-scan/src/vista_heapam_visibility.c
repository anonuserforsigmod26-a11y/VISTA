#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#include "vista_heapam_visibility.h"
#include "vista_ro_slru.h"

static __thread bool slru_inited = false;
static __thread VistaSlruCtl VistaXactCtl;
static __thread VistaSlruCtl VistaSubXactCtl;
static __thread VistaSlruCtl VistaMXactOffsetCtl;
static __thread VistaSlruCtl VistaMXactMemberCtl;
static __thread VistaTransactionId VistaCachedFetchXid = VistaInvalidTransactionId;
static __thread VistaXidStatus VistaCachedFetchXidStatus;

static void
VistaInitSlruCtls(void)
{
	VistaXactCtl = (VistaSlruCtl)malloc(sizeof(struct VistaSlruCtlData));
	VistaSubXactCtl = (VistaSlruCtl)malloc(sizeof(struct VistaSlruCtlData));
	VistaMXactOffsetCtl = (VistaSlruCtl)malloc(sizeof(struct VistaSlruCtlData));
	VistaMXactMemberCtl = (VistaSlruCtl)malloc(sizeof(struct VistaSlruCtlData));

	VistaSlruInit(VistaXactCtl, "pg_xact", 128, "pg_xact");
	VistaSlruInit(VistaSubXactCtl, "pg_subtrans", 32, "pg_subtrans");
	VistaSlruInit(VistaMXactOffsetCtl, "pg_multixact/offsets", 8, "pg_multixact/offsets");
	VistaSlruInit(VistaMXactMemberCtl, "pg_multixact/members", 16, "pg_multixact/members");
	VistaCachedFetchXid = VistaInvalidTransactionId;
	VistaCachedFetchXidStatus = (VistaXidStatus)-1;
}

static void
VistaResetSlruCtls(void)
{
	VistaSlruReset(VistaXactCtl);
	VistaSlruReset(VistaSubXactCtl);
	VistaSlruReset(VistaMXactOffsetCtl);
	VistaSlruReset(VistaMXactMemberCtl);
	VistaCachedFetchXid = VistaInvalidTransactionId;
	VistaCachedFetchXidStatus = (VistaXidStatus)-1;
}

static bool
VistaTransactionIdPrecedes(VistaTransactionId id1, VistaTransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32_t		diff;

	if (!VistaTransactionIdIsNormal(id1) || !VistaTransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32_t) (id1 - id2);
	return (diff < 0);
}

static bool
VistaTransactionIdFollowsOrEquals(VistaTransactionId id1, VistaTransactionId id2)
{
	int32_t		diff;

	if (!VistaTransactionIdIsNormal(id1) || !VistaTransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32_t) (id1 - id2);
	return (diff >= 0);
}

static VistaXidStatus
VistaTransactionIdGetStatus(VistaTransactionId xid)
{
	int			pageno = VistaTransactionIdToPage(xid);
	int			byteno = VistaTransactionIdToByte(xid);
	int			bshift = VistaTransactionIdToBIndex(xid) * VISTA_CLOG_BITS_PER_XACT;
	char	    *byteptr;
	char		*buf;
	VistaXidStatus	status;

	buf = (char *)VistaSlruReadPage(VistaXactCtl, pageno);

	byteptr = buf + byteno;
	status = (*byteptr >> bshift) & VISTA_CLOG_XACT_BITMASK;

	return status;
}

static VistaTransactionId
VistaSubTransGetParent(VistaTransactionId xid)
{
	int			pageno = VistaSubTransactionIdToPage(xid);
	int			entryno = VistaSubTransactionIdToEntry(xid);
	char		*buf;
	VistaTransactionId *ptr;
	VistaTransactionId parent;

	/* Bootstrap and frozen XIDs have no parent */
	if (!VistaTransactionIdIsNormal(xid))
		return VistaInvalidTransactionId;

	buf = (char *)VistaSlruReadPage(VistaSubXactCtl, pageno);

	ptr = (VistaTransactionId *)buf;
	ptr += entryno;
	parent = *ptr;

	return parent;
}


static VistaTransactionId
VistaSubTransGetTopmostTransaction(VistaTransactionId xid, VistaTransactionId snapshot_xmin)
{
	VistaTransactionId parentXid = xid,
					   previousXid = xid;

	while (VistaTransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;

		//! [VISTA] Originally, this code checks with TransactionXmin of the backend which called this function.
		//! We changed it to use a illegal snapshot->xmin. (I like illegal.)
		//! This code assumes GC will not be happend in postgreSQL.
		if (VistaTransactionIdPrecedes(parentXid, snapshot_xmin))
			break;

		parentXid = VistaSubTransGetParent(parentXid);

		/*
		 * By convention the parent xid gets allocated first, so should always
		 * precede the child xid. Anything else points to a corrupted data
		 * structure that could lead to an infinite loop, so exit.
		 */
		if (!VistaTransactionIdPrecedes(parentXid, previousXid)) {
			//! [TODO] error print
		}
	}

	Assert(VistaTransactionIdIsValid(previousXid));

	return previousXid;
}

static bool
VistaXidInMVCCSnapshot(VistaTransactionId xid, VistaSnapshot snapshot)
{
	uint32_t		i;

	/*
	 * Make a quick range check to eliminate most XIDs without looking at the
	 * xip arrays.  Note that this is OK even if we convert a subxact XID to
	 * its parent below, because a subxact with XID < xmin has surely also got
	 * a parent with XID < xmin, while one with XID >= xmax must belong to a
	 * parent that was not yet committed at the time of this snapshot.
	 */

	/* Any xid < xmin is not in-progress */
	if (VistaTransactionIdPrecedes(xid, snapshot->xmin))
		return false;
	/* Any xid >= xmax is in-progress */
	if (VistaTransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return true;

	/*
	 * Snapshot information is stored slightly differently in snapshots taken
	 * during recovery.
	 */
	if (!snapshot->takenDuringRecovery)
	{
		/*
		 * If the snapshot contains full subxact data, the fastest way to
		 * check things is just to compare the given XID against both subxact
		 * XIDs and top-level XIDs.  If the snapshot overflowed, we have to
		 * use pg_subtrans to convert a subxact XID to its parent XID, but
		 * then we need only look at top-level XIDs not subxacts.
		 */
		if (!snapshot->suboverflowed)
		{
			/* we have full data, so search subxip */
			int32_t		j;

			for (j = 0; j < snapshot->subxcnt; j++)
			{
				if (VistaTransactionIdEquals(xid, snapshot->subxip[j]))
					return true;
			}

			/* not there, fall through to search xip[] */
		}
		else
		{
			/*
			 * Snapshot overflowed, so convert xid to top-level.  This is safe
			 * because we eliminated too-old XIDs above.
			 */

			xid = VistaSubTransGetTopmostTransaction(xid, snapshot->xmin);

			/*
			 * If xid was indeed a subxact, we might now have an xid < xmin,
			 * so recheck to avoid an array scan.  No point in rechecking
			 * xmax.
			 */
			if (VistaTransactionIdPrecedes(xid, snapshot->xmin))
				return false;
		}

		for (i = 0; i < snapshot->xcnt; i++)
		{
			if (VistaTransactionIdEquals(xid, snapshot->xip[i]))
				return true;
		}
	}
	else
	{
		int32_t		j;

		/*
		 * In recovery we store all xids in the subxact array because it is by
		 * far the bigger array, and we mostly don't know which xids are
		 * top-level and which are subxacts. The xip array is empty.
		 *
		 * We start by searching subtrans, if we overflowed.
		 */

		if (snapshot->suboverflowed)
		{
			/*
			 * Snapshot overflowed, so convert xid to top-level.  This is safe
			 * because we eliminated too-old XIDs above.
			 */
			xid = VistaSubTransGetTopmostTransaction(xid, snapshot->xmin);

			/*
			 * If xid was indeed a subxact, we might now have an xid < xmin,
			 * so recheck to avoid an array scan.  No point in rechecking
			 * xmax.
			 */
			if (VistaTransactionIdPrecedes(xid, snapshot->xmin))
				return false;
		}

		/*
		 * We now have either a top-level xid higher than xmin or an
		 * indeterminate xid. We don't know whether it's top level or subxact
		 * but it doesn't matter. If it's present, the xid is visible.
		 */
		for (j = 0; j < snapshot->subxcnt; j++)
		{
			if (VistaTransactionIdEquals(xid, snapshot->subxip[j]))
				return true;
		}
	}

	return false;
}

static VistaXidStatus
VistaTransactionLogFetch(VistaTransactionId transactionId)
{
	VistaXidStatus	xidstatus;

	/*
	 * Before going to the commit log manager, check our single item cache to
	 * see if we didn't just check the transaction status a moment ago.
	 */
	if (VistaTransactionIdEquals(transactionId, VistaCachedFetchXid))
		return VistaCachedFetchXidStatus;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!VistaTransactionIdIsNormal(transactionId))
	{
		if (VistaTransactionIdEquals(transactionId, VistaBootstrapTransactionId))
			return VISTA_TRANSACTION_STATUS_COMMITTED;
		if (VistaTransactionIdEquals(transactionId, VistaFrozenTransactionId))
			return VISTA_TRANSACTION_STATUS_COMMITTED;
		return VISTA_TRANSACTION_STATUS_ABORTED;
	}

	/*
	 * Get the transaction status.
	 */
	xidstatus = VistaTransactionIdGetStatus(transactionId);

	/*
	 * Cache it, but DO NOT cache status for unfinished or sub-committed
	 * transactions!  We only cache status that is guaranteed not to change.
	 */
	if (xidstatus != VISTA_TRANSACTION_STATUS_IN_PROGRESS &&
		xidstatus != VISTA_TRANSACTION_STATUS_SUB_COMMITTED &&
		xidstatus != (VistaXidStatus)-1)
	{
		VistaCachedFetchXid = transactionId;
		VistaCachedFetchXidStatus = xidstatus;
	}

	return xidstatus;
}

static bool							/* true if given transaction committed */
VistaTransactionIdDidCommit(VistaTransactionId transactionId, VistaTransactionId snapshot_xmin)
{
	VistaXidStatus	xidstatus;

	//! [VISTA] CLOG read.
	xidstatus = VistaTransactionLogFetch(transactionId);

	/*
	 * If it's marked committed, it's committed.
	 */
	if (xidstatus == VISTA_TRANSACTION_STATUS_COMMITTED)
		return true;

	if (xidstatus == VISTA_TRANSACTION_STATUS_SUB_COMMITTED)
	{
		VistaTransactionId parentXid;

		if (VistaTransactionIdPrecedes(transactionId, snapshot_xmin))
			return false;

		parentXid = VistaSubTransGetParent(transactionId);
		if (!VistaTransactionIdIsValid(parentXid))
		{
			//! [TODO] error print.
			return false;
		}
		return VistaTransactionIdDidCommit(parentXid, snapshot_xmin);
	}

	/*
	 * It's not committed.
	 */
	return false;
}

static int
VistaMXactCacheGetById(VistaMultiXactId multi, VistaMultiXactMember **members)
{
	//! [TODO] Imitate the process of the MXactCache.
	//! For now, we always return -1 to force reading from storage
	return -1;
}

static VistaMultiXactOffset
VistaReadMultiXactOffset(VistaMultiXactId multi)
{
	int			pageno = VistaMultiXactIdToOffsetPage(multi);
	int			entryno = VistaMultiXactIdToOffsetEntry(multi);
	char		*buf;
	VistaMultiXactOffset *offptr;
	VistaMultiXactOffset result;

	buf = (char *)VistaSlruReadPage(VistaMXactOffsetCtl, pageno);

	offptr = (VistaMultiXactOffset *)buf;
	offptr += entryno;
	result = *offptr;

	return result;
}

static int
VistaGetMultiXactIdMembers(VistaMultiXactId multi, VistaMultiXactMember **members)
{
	VistaMultiXactOffset offset;
	VistaMultiXactOffset nextOffset;
	VistaMultiXactId nextMXact;
	int			length;
	int			truelength;
	int			i;
	VistaMultiXactMember *ptr;
	int			prev_pageno;
	char		*buf;

	if (!VistaMultiXactIdIsValid(multi))
	{
		*members = NULL;
		return -1;
	}

	/* See if the VistaMultiXactId is in the local cache */
	length = VistaMXactCacheGetById(multi, members);
	if (length >= 0)
		return length;

	/* Read the offset for this multixact */
	fprintf(stderr, "Reading offset for multixact %u\n", multi);
	offset = VistaReadMultiXactOffset(multi);
	if (offset == 0)
	{
		*members = NULL;
		return -1;
	}

	/* 
	 * To determine the length, we need to read the next multixact's offset.
	 * For simplicity, we'll try the next multixact ID.
	 */
	nextMXact = multi + 1;
	
	/* Handle wraparound if needed */
	if (nextMXact < VistaFirstMultiXactId)
		nextMXact = VistaFirstMultiXactId;

	nextOffset = VistaReadMultiXactOffset(nextMXact);
	if (nextOffset == 0)
	{
		/* 
		 * If we can't read the next offset, assume a reasonable maximum.
		 * This is a simplification - in production code you'd want better handling.
		 */
		length = 64; /* Reasonable default max members */
	}
	else
	{
		length = nextOffset - offset;
	}

	/* Sanity check on length */
	if (length <= 0 || length > 1000) /* Reasonable upper bound */
	{
		*members = NULL;
		return -1;
	}

	ptr = (VistaMultiXactMember *) malloc(length * sizeof(VistaMultiXactMember));
	if (!ptr)
	{
		*members = NULL;
		return -1;
	}

	/* Now get the members themselves */
	truelength = 0;
	prev_pageno = -1;
	
	for (i = 0; i < length; i++, offset++)
	{
		VistaTransactionId *xactptr;
		uint32_t	   *flagsptr;
		int			flagsoff;
		int			bshift;
		int			memberoff;
		int			pageno;

		pageno = VistaMXOffsetToMemberPage(offset);
		memberoff = VistaMXOffsetToMemberOffset(offset);

		if (pageno != prev_pageno)
		{
			buf = (char *)VistaSlruReadPage(VistaMXactMemberCtl, pageno);
			prev_pageno = pageno;
		}

		xactptr = (VistaTransactionId *) (buf + memberoff);

		if (!VistaTransactionIdIsValid(*xactptr))
		{
			/* Corner case: we must be looking at unused slot zero */
			if (offset == 0)
				continue;
			/* Otherwise, we've reached the end of valid members */
			break;
		}

		flagsoff = VistaMXOffsetToFlagsOffset(offset);
		bshift = VistaMXOffsetToFlagsBitShift(offset);
		flagsptr = (uint32_t *) (buf + flagsoff);

		ptr[truelength].xid = *xactptr;
		ptr[truelength].status = (*flagsptr >> bshift) & VISTA_MXACT_MEMBER_XACT_BITMASK;
		truelength++;
	}

	/* A multixid with zero members should not happen */
	if (truelength == 0)
	{
		free(ptr);
		*members = NULL;
		return -1;
	}

	*members = ptr;
	return truelength;
}

static VistaTransactionId
VistaMultiXactIdGetUpdateXid(VistaTransactionId xmax, uint16_t t_infomask)
{
	VistaTransactionId update_xact = VistaInvalidTransactionId;
	VistaMultiXactMember *members;
	int			nmembers;

	Assert(!(t_infomask & VISTA_HEAP_XMAX_LOCK_ONLY));
	Assert(t_infomask & VISTA_HEAP_XMAX_IS_MULTI);

	/*
	 * Since we know the LOCK_ONLY bit is not set, this cannot be a multi from
	 * pre-pg_upgrade.
	 */
	nmembers = VistaGetMultiXactIdMembers(xmax, &members);

	if (nmembers > 0)
	{
		int			i;

		for (i = 0; i < nmembers; i++)
		{
			/* Ignore lockers */
			if (!VISTA_ISUPDATE_from_mxstatus(members[i].status))
				continue;

			/* there can be at most one updater */
			/* Assert(update_xact == VistaInvalidTransactionId); */
			update_xact = members[i].xid;
		}

		free(members);
	}

	return update_xact;
}

static VistaTransactionId
VistaHeapTupleGetUpdateXid(VistaHeapTupleHeader tuple)
{
	return VistaMultiXactIdGetUpdateXid(VistaHeapTupleHeaderGetRawXmax(tuple),
										tuple->t_infomask);
}

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 * 1. There are no SetHintBits().
 * 2. The `htup` never can be edited by the OLAP engine which would call this function.
 *    Therefore, we dropped the TransactionIdIsCurrentTransactionId() branches. 
 * 3. No backward compatability with pre-9.0 binaries.
 *
 * For supporting safe cache, we added `all_visible` output argument.
 * `all_visible` will be set to true if this tuple is visible to all future transactions.

 * Case 1. |<----- ver 1 ----->|<----- ver 2 ----->|<----- ver latest ---.... (Update)
 *                                                       ^
 *                                                       |
 *                                                   all_visible = true
 * Case 2. |<----- ver 1 ----->|<----- ver 2 ----->|                          (Delete)
 *                                                       ^
 *                                                       |
 *                                                   all_visible = true
 */
bool
VistaHeapTupleSatisfiesMVCC(VistaHeapTuple htup, VistaSnapshot snapshot, int *all_visible)
{
	VistaHeapTupleHeader tuple = htup->t_data;

	if (unlikely(!slru_inited)) {
		VistaInitSlruCtls(); // Lazy init
		slru_inited = true;
	}

	Assert(VistaItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != VistaInvalidOid);

	// No hint bit for Xmin committed
	if (!VistaHeapTupleHeaderXminCommitted(tuple)) 
	{
		/* This tuple was inserted by an aborted transaction (hintbit) */ 
		if (VistaHeapTupleHeaderXminInvalid(tuple)) {
			*all_visible = true; // Future snapshots also cannot see this tuple.
			return false;
		}

		// If the inserting transaction is still in progress according to our snapshot, the tuple is invisible.
		if (VistaXidInMVCCSnapshot(VistaHeapTupleHeaderGetRawXmin(tuple), snapshot)) {
			*all_visible = false; // Future snapshot may see this tuple.
			return false;

		// If the xmin is not in-progress, and it is committed, we should move to check xmax.
		} else if (VistaTransactionIdDidCommit(VistaHeapTupleHeaderGetRawXmin(tuple), snapshot->xmin)) {
			// Do nothing, move to check xmax.

		// If the xmin is aborted, the tuple is invisible to all future snapshots.
		} else {
			*all_visible = true;
			return false;
		}
	}
	else // Hint bit says Xmin committed
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!VistaHeapTupleHeaderXminFrozen(tuple) &&
			VistaXidInMVCCSnapshot(VistaHeapTupleHeaderGetRawXmin(tuple), snapshot)) {
			*all_visible = false;
			return false;		/* treat as still in progress */
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & VISTA_HEAP_XMAX_INVALID) {
		*all_visible = true; /* This is the latest tuple and we can see it according to the snapshot */
		return true;
	}

	if (VISTA_HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		*all_visible = true; /* Cache may be invalidated in near future, but, this
							  * is the logically latest tuple that all future
							  * snapshots can see.
							  * xmax will be updated after commit/abort, so it is safe.
							  */
		return true;
	}

	// Hint bit says Xmax is multi-transaction id.
	if (tuple->t_infomask & VISTA_HEAP_XMAX_IS_MULTI)
	{
		VistaTransactionId xmax;

		/* already checked above */
		Assert(!VISTA_HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = VistaHeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(VistaTransactionIdIsValid(xmax));

		if (VistaXidInMVCCSnapshot(xmax, snapshot)) {
			*all_visible = false; // Future snapshot may cannot see this tuple.
			return true;
		}

		if (VistaTransactionIdDidCommit(xmax, snapshot->xmin)) {
			*all_visible = true; /* There may be a newer version, but at least this version is invisible to all future snapshots */
			return false;		/* updating transaction committed */
		}

		/* it must have aborted or crashed */
		*all_visible = true; /* So, this is the latest tuple that future snapshots can see */
		return true;
	}

	// No hint bit for Xmax committed
	if (!(tuple->t_infomask & VISTA_HEAP_XMAX_COMMITTED))
	{
		if (VistaXidInMVCCSnapshot(VistaHeapTupleHeaderGetRawXmax(tuple), snapshot)) {
			*all_visible = false; // Future snapshot may cannot see this tuple.
			return true;
		}

		if (!VistaTransactionIdDidCommit(VistaHeapTupleHeaderGetRawXmax(tuple), snapshot->xmin)) {
			*all_visible = true; /* This is the latest tuple that all future snapshots can see */
			return true;
		}
	}
	else // xmax is committed.
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (VistaXidInMVCCSnapshot(VistaHeapTupleHeaderGetRawXmax(tuple), snapshot)) {
			*all_visible = false;
			return true;		/* treat as still in progress */
		}
	}

	/* xmax transaction committed */
	*all_visible = true; /* There may be a newer version, but at least this version is invisible to all future snapshots */
	return false;
}

#endif /* _GNU_SOURCE */