#ifndef __HEAPAM_VISIBILITY_H
#define __HEAPAM_VISIBILITY_H

#include "file_access.h"

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#define VISTA_PG_PATH (POSTGRES_DATA_DIR)

/* Simple assert macro for debugging */
#ifdef DEBUG
#define Assert(condition) \
	do { \
		if (!(condition)) { \
			fprintf(stderr, "Assertion failed: %s, file %s, line %d\n", \
					#condition, __FILE__, __LINE__); \
			abort(); \
		} \
	} while (0)
#else
#define Assert(condition) ((void)0)
#endif

typedef uint32_t VistaTransactionId;
typedef uint32_t VistaCommandId;
typedef int64_t VistaTimestampTz;
typedef uint64_t VistaXLogRecPtr;
typedef uint32_t VistaMultiXactOffset;
typedef VistaTransactionId VistaMultiXactId;
typedef int VistaXidStatus;
typedef uint32_t VistaMultiXactOffset;

#define VISTA_MAXPGPATH (1024)
#define VISTA_SLRU_PAGES_PER_SEGMENT (32)

#define VistaInvalidTransactionId		((VistaTransactionId) 0)
#define VistaBootstrapTransactionId		((VistaTransactionId) 1)
#define VistaFrozenTransactionId		((VistaTransactionId) 2)
#define VistaFirstNormalTransactionId	((VistaTransactionId) 3)

#define VISTA_FLEXIBLE_ARRAY_MEMBER	/* empty */

#define VISTA_TRANSACTION_STATUS_IN_PROGRESS		(0x00)
#define VISTA_TRANSACTION_STATUS_COMMITTED			(0x01)
#define VISTA_TRANSACTION_STATUS_ABORTED			(0x02)
#define VISTA_TRANSACTION_STATUS_SUB_COMMITTED		(0x03)

#define VISTA_MULTIXACT_OFFSETS_PER_PAGE (VISTA_BLCKSZ / sizeof(VistaMultiXactOffset))

#define VistaMultiXactIdToOffsetPage(xid) \
	((xid) / (VistaMultiXactOffset) VISTA_MULTIXACT_OFFSETS_PER_PAGE)
#define VistaMultiXactIdToOffsetEntry(xid) \
	((xid) % (VistaMultiXactOffset) VISTA_MULTIXACT_OFFSETS_PER_PAGE)

/* MultiXact member constants */
#define VISTA_MXACT_MEMBER_BITS_PER_XACT		8
#define VISTA_MXACT_MEMBER_FLAGS_PER_BYTE		1
#define VISTA_MXACT_MEMBER_XACT_BITMASK			((1 << VISTA_MXACT_MEMBER_BITS_PER_XACT) - 1)
#define VISTA_MULTIXACT_FLAGBYTES_PER_GROUP		4
#define VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP	\
	(VISTA_MULTIXACT_FLAGBYTES_PER_GROUP * VISTA_MXACT_MEMBER_FLAGS_PER_BYTE)
#define VISTA_MULTIXACT_MEMBERGROUP_SIZE \
	(sizeof(VistaTransactionId) * VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP + VISTA_MULTIXACT_FLAGBYTES_PER_GROUP)
#define VISTA_MULTIXACT_MEMBERGROUPS_PER_PAGE (VISTA_BLCKSZ / VISTA_MULTIXACT_MEMBERGROUP_SIZE)
#define VISTA_MULTIXACT_MEMBERS_PER_PAGE	\
	(VISTA_MULTIXACT_MEMBERGROUPS_PER_PAGE * VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP)

#define VistaMXOffsetToMemberPage(xid) ((xid) / (VistaTransactionId) VISTA_MULTIXACT_MEMBERS_PER_PAGE)
#define VistaMXOffsetToMemberOffset(xid) \
	(VistaMXOffsetToFlagsOffset(xid) + VISTA_MULTIXACT_FLAGBYTES_PER_GROUP + \
	 ((xid) % VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP) * sizeof(VistaTransactionId))
#define VistaMXOffsetToFlagsOffset(xid) \
	((((xid) / (VistaTransactionId) VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP) % \
	  (VistaTransactionId) VISTA_MULTIXACT_MEMBERGROUPS_PER_PAGE) * \
	 (VistaTransactionId) VISTA_MULTIXACT_MEMBERGROUP_SIZE)
#define VistaMXOffsetToFlagsBitShift(xid) \
	(((xid) % (VistaTransactionId) VISTA_MULTIXACT_MEMBERS_PER_MEMBERGROUP) * \
	 VISTA_MXACT_MEMBER_BITS_PER_XACT)

#define VISTA_ISUPDATE_from_mxstatus(status) \
	((status) > VistaMultiXactStatusForUpdate)

#define VistaInvalidOid ((VistaOid) 0)

typedef enum
{
	VistaMultiXactStatusForKeyShare = 0x00,
	VistaMultiXactStatusForShare = 0x01,
	VistaMultiXactStatusForNoKeyUpdate = 0x02,
	VistaMultiXactStatusForUpdate = 0x03,
	/* an update that doesn't touch "key" columns */
	VistaMultiXactStatusNoKeyUpdate = 0x04,
	/* other updates, and delete */
	VistaMultiXactStatusUpdate = 0x05
} VistaMultiXactStatus;

typedef struct VistaMultiXactMember
{
	VistaTransactionId xid;
	VistaMultiXactStatus status;
} VistaMultiXactMember;

#define VistaInvalidMultiXactId	((VistaMultiXactId) 0)
#define VistaFirstMultiXactId	((VistaMultiXactId) 1)
#define VistaMultiXactIdIsValid(multi) ((multi) != VistaInvalidMultiXactId)

typedef struct vista_pairingheap_node
{
	struct vista_pairingheap_node *first_child;
	struct vista_pairingheap_node *next_sibling;
	struct vista_pairingheap_node *prev_or_parent;
} vista_pairingheap_node;

typedef struct VistaFullTransactionId
{
	uint64_t		value;
} VistaFullTransactionId;

struct GlobalVisState
{
	/* XIDs >= are considered running by some backend */
	VistaFullTransactionId definitely_needed;

	/* XIDs < are not considered to be running by any backend */
	VistaFullTransactionId maybe_needed;
};

typedef enum VistaSnapshotType
{
	VISTA_SNAPSHOT_MVCC = 0,
	VISTA_SNAPSHOT_SELF,
	VISTA_SNAPSHOT_ANY,
	VISTA_SNAPSHOT_TOAST,
	VISTA_SNAPSHOT_DIRTY,
	VISTA_SNAPSHOT_HISTORIC_MVCC,
	VISTA_SNAPSHOT_NON_VACUUMABLE
} VistaSnapshotType;

typedef struct VistaSnapshotData
{
	VistaSnapshotType snapshot_type; /* type of snapshot */

	VistaTransactionId xmin;			/* all XID < xmin are visible to me */
	VistaTransactionId xmax;			/* all XID >= xmax are invisible to me */

	VistaTransactionId *xip;
	uint32_t		xcnt;			/* # of xact ids in xip[] */

	VistaTransactionId *subxip;
	int32_t		subxcnt;		/* # of xact ids in subxip[] */
	bool		suboverflowed;	/* has the subxip array overflowed? */

	bool		takenDuringRecovery;	/* recovery-shaped snapshot? */
	bool		copied;			/* false if it's a static snapshot */

	VistaCommandId	curcid;			/* in my xact, CID < curcid are visible */

	uint32_t		speculativeToken;

	struct GlobalVisState *vistest;

	uint32_t		active_count;	/* refcount on ActiveSnapshot stack */
	uint32_t		regd_count;		/* refcount on RegisteredSnapshots */
	vista_pairingheap_node ph_node;	/* link in the RegisteredSnapshots heap */

	VistaTimestampTz whenTaken;		/* timestamp when snapshot was taken */
	VistaXLogRecPtr	lsn;			/* position in the WAL stream when taken */

	uint64_t		snapXactCompletionCount;
} VistaSnapshotData;
typedef struct VistaSnapshotData *VistaSnapshot;

typedef struct VistaBlockIdData
{
	uint16_t		bi_hi;
	uint16_t		bi_lo;
} VistaBlockIdData;

typedef uint16_t VistaOffsetNumber;
typedef struct VistaItemPointerData
{
	VistaBlockIdData ip_blkid;
	VistaOffsetNumber ip_posid;
}
/* If compiler understands packed and aligned pragmas, use those */
__attribute__((__packed__, __aligned__(2)))
VistaItemPointerData;

// #if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
// 			pg_attribute_packed()
// 			pg_attribute_aligned(2)
// #endif

typedef VistaItemPointerData *VistaItemPointer;

typedef unsigned int VistaOid;

typedef struct VistaDatumTupleFields
{
	int32_t		datum_len_;		/* varlena header (do not touch directly!) */

	int32_t		datum_typmod;	/* -1, or identifier of a record type */

	VistaOid			datum_typeid;	/* composite type OID, or RECORDOID */
} VistaDatumTupleFields;

typedef struct VistaHeapTupleFields
{
	VistaTransactionId t_xmin;		/* inserting xact ID */
	VistaTransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		VistaCommandId	t_cid;		/* inserting or deleting command ID, or both */
		VistaTransactionId t_xvac;	/* old-style VACUUM FULL xact ID */
	}			t_field3;
} VistaHeapTupleFields;

struct VistaHeapTupleHeaderData
{
	union
	{
		VistaHeapTupleFields t_heap;
		VistaDatumTupleFields t_datum;
	}			t_choice;

	VistaItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16_t		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16_t		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8_t		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
	uint8_t		t_bits[VISTA_FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

typedef struct VistaHeapTupleHeaderData VistaHeapTupleHeaderData;

typedef VistaHeapTupleHeaderData *VistaHeapTupleHeader;

typedef struct VistaHeapTupleData
{
	uint32_t		t_len;			/* length of *t_data */
	VistaItemPointerData t_self;		/* SelfItemPointer */
	VistaOid			t_tableOid;		/* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
	VistaHeapTupleHeader t_data;		/* -> tuple header and data */
} VistaHeapTupleData;

typedef VistaHeapTupleData *VistaHeapTuple;

#define VistaItemPointerIsValid(pointer) \
	((bool) ((const void*)(pointer) != NULL && ((pointer)->ip_posid != 0)))

#define VISTA_HEAP_XMIN_COMMITTED 	(0x0100)
#define VISTA_HEAP_XMIN_INVALID 	(0x0200)
#define VISTA_HEAP_XMAX_INVALID		(0x0800)
#define VISTA_HEAP_XMAX_IS_MULTI	(0x1000)
#define VISTA_HEAP_XMIN_FROZEN		(VISTA_HEAP_XMIN_COMMITTED|VISTA_HEAP_XMIN_INVALID)
#define VISTA_HEAP_XMAX_EXCL_LOCK	(0x0040)
#define VISTA_HEAP_XMAX_KEYSHR_LOCK	(0x0010)
#define VISTA_HEAP_XMAX_LOCK_ONLY	(0x0080)
#define VISTA_HEAP_XMAX_COMMITTED	(0x0400)

#define VISTA_HEAP_XMAX_SHR_LOCK	(VISTA_HEAP_XMAX_EXCL_LOCK | VISTA_HEAP_XMAX_KEYSHR_LOCK)
#define VISTA_HEAP_LOCK_MASK		(VISTA_HEAP_XMAX_SHR_LOCK | VISTA_HEAP_XMAX_EXCL_LOCK | \
						 			VISTA_HEAP_XMAX_KEYSHR_LOCK)

#define VISTA_HEAP_XMAX_IS_LOCKED_ONLY(infomask) \
	(((infomask) & VISTA_HEAP_XMAX_LOCK_ONLY) || \
	 (((infomask) & (VISTA_HEAP_XMAX_IS_MULTI | VISTA_HEAP_LOCK_MASK)) == VISTA_HEAP_XMAX_EXCL_LOCK))

#define VistaHeapTupleHeaderXminCommitted(tup) \
( \
	((tup)->t_infomask & VISTA_HEAP_XMIN_COMMITTED) != 0 \
)

#define VistaHeapTupleHeaderXminInvalid(tup) \
( \
	((tup)->t_infomask & (VISTA_HEAP_XMIN_COMMITTED|VISTA_HEAP_XMIN_INVALID)) == \
		VISTA_HEAP_XMIN_INVALID \
)

#define VistaHeapTupleHeaderGetRawXmin(tup) \
( \
	(tup)->t_choice.t_heap.t_xmin \
)

#define VistaHeapTupleHeaderGetRawXmax(tup) \
( \
	(tup)->t_choice.t_heap.t_xmax \
)

#define VistaHeapTupleHeaderXminFrozen(tup) \
( \
	((tup)->t_infomask & (VISTA_HEAP_XMIN_FROZEN)) == VISTA_HEAP_XMIN_FROZEN \
)

#define VistaTransactionIdIsValid(xid)		((xid) != VistaInvalidTransactionId)
#define VistaTransactionIdIsNormal(xid)		((xid) >= VistaFirstNormalTransactionId)

#define VistaTransactionIdEquals(id1, id2)	((id1) == (id2))

#define VISTA_BLCKSZ (8192)
#define VISTA_CLOG_BITS_PER_XACT	(2)
#define VISTA_CLOG_XACTS_PER_BYTE 	(4)
#define VISTA_CLOG_XACT_BITMASK	((1 << VISTA_CLOG_BITS_PER_XACT) - 1)
#define VISTA_CLOG_XACTS_PER_PAGE (VISTA_BLCKSZ * VISTA_CLOG_XACTS_PER_BYTE)
#define VISTA_SUBTRANS_XACTS_PER_PAGE (VISTA_BLCKSZ / sizeof(VistaTransactionId))
#define VistaTransactionIdToPage(xid) ((xid) / (VistaTransactionId) VISTA_CLOG_XACTS_PER_PAGE)
#define VistaTransactionIdToPgIndex(xid) ((xid) % (VistaTransactionId) VISTA_CLOG_XACTS_PER_PAGE)
#define VistaTransactionIdToByte(xid)   (VistaTransactionIdToPgIndex(xid) / VISTA_CLOG_XACTS_PER_BYTE)
#define VistaTransactionIdToBIndex(xid) ((xid) % (VistaTransactionId) VISTA_CLOG_XACTS_PER_BYTE)
#define VistaTransactionIdToEntry(xid) ((xid) % (VistaTransactionId) VISTA_CLOG_XACTS_PER_PAGE)

#define VistaSubTransactionIdToPage(xid) ((xid) / (VistaTransactionId) VISTA_SUBTRANS_XACTS_PER_PAGE)
#define VistaSubTransactionIdToEntry(xid) ((xid) % (VistaTransactionId) VISTA_SUBTRANS_XACTS_PER_PAGE)

#ifdef __cplusplus
extern "C" {
#endif
/* Visibility Checking API */
bool VistaHeapTupleSatisfiesMVCC(VistaHeapTuple htup, VistaSnapshot snapshot, int *all_visible);
#ifdef __cplusplus
}
#endif

#endif /* __HEAPAM_VISIBILITY_H */