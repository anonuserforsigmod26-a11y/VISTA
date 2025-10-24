#ifndef PG_MACROS_H
#define PG_MACROS_H

#include <string.h>
#include "pg_types.h"

/* Min/Max macros */
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))

/* Generic alignment macro */
#define TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

/* Platform-specific alignment macros */
#define SHORTALIGN(LEN)         TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)           TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)          TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)        TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)           TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* Heap tuple macros */
#define HeapTupleHasNulls(tuple) \
    (((tuple)->t_data->t_infomask & HEAP_HASNULL) != 0)
#define HeapTupleHeaderGetNatts(tup) \
    ((tup)->t_infomask2 & HEAP_NATTS_MASK)
#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])
#define TTS_SLOW(slot) (((slot)->tts_flags & TTS_FLAG_SLOW) != 0)

/* Bit manipulation for null bitmap */
#define att_isnull(ATT, BITS) (!((BITS)[(ATT) >> 3] & (1 << ((ATT) & 0x07))))

/* AssertMacro for debugging - simplified version */
#define AssertMacro(condition) ((void)(condition))

/* VARATT_NOT_PAD_BYTE - check if pointer is not a padding byte */
#define VARATT_NOT_PAD_BYTE(PTR) \
    (*((uint8 *) (PTR)) != 0)

/* Alignment macros */
#define att_align_nominal(cur_offset, attalign) \
( \
    ((attalign) == 'i') ? INTALIGN(cur_offset) : \
    (((attalign) == 'c') ? (uintptr_t) (cur_offset) : \
    (((attalign) == 'd') ? DOUBLEALIGN(cur_offset) : \
    ( \
        AssertMacro((attalign) == 's'), \
        SHORTALIGN(cur_offset) \
    ))) \
)

#define att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
    ((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
    (uintptr_t) (cur_offset) : \
    att_align_nominal(cur_offset, attalign) \
)

/* Variable-length (varlena) structures - PostgreSQL standard */
typedef union
{
    struct                      /* Normal varlena (4-byte length) */
    {
        uint32      va_header;
        char        va_data[FLEXIBLE_ARRAY_MEMBER];
    }           va_4byte;
    struct                      /* Compressed-in-line format */
    {
        uint32      va_header;
        uint32      va_tcinfo;  /* Original data size and compression method */
        char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    }           va_compressed;
} varattrib_4b;

typedef struct
{
    uint8       va_header;
    char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

typedef struct
{
    uint8       va_header;      /* Always 0x80 or 0x01 */
    uint8       va_tag;         /* Type of datum */
    char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

/* Varlena header constants */
#ifndef VARHDRSZ
#define VARHDRSZ 4
#endif
#define VARHDRSZ_SHORT      offsetof(varattrib_1b, va_data)
#define VARHDRSZ_EXTERNAL   offsetof(varattrib_1b_e, va_data)

/* Endian-dependent macros for little-endian systems */
#define VARATT_IS_4B(PTR) \
    ((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
    ((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
    ((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
    ((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
    ((((varattrib_1b *) (PTR))->va_header) == 0x01)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
    ((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
    ((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
    (((varattrib_1b_e *) (PTR))->va_tag)

#define VARDATA_4B(PTR)     (((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_1B(PTR)     (((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)   (((varattrib_1b_e *) (PTR))->va_data)

/* Externally visible TOAST macros */
#define VARDATA(PTR)                        VARDATA_4B(PTR)
#define VARSIZE(PTR)                        VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR)                  VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)                  VARDATA_1B(PTR)

#define VARATT_IS_COMPRESSED(PTR)           VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR)             VARATT_IS_1B_E(PTR)
#define VARATT_IS_SHORT(PTR)                VARATT_IS_1B(PTR)
#define VARATT_IS_EXTENDED(PTR)             (!VARATT_IS_4B_U(PTR))

#define VARSIZE_ANY(PTR) \
    (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
     (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
      VARSIZE_4B(PTR)))

/* Size of a varlena data, excluding header */
#define VARSIZE_ANY_EXHDR(PTR) \
    (VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL : \
     (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
      VARSIZE_4B(PTR)-VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
#define VARDATA_ANY(PTR) \
     (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

/* Forward declarations for TOAST structures */
typedef struct ExpandedObjectHeader ExpandedObjectHeader;

/* TOAST pointer structures */
typedef struct varatt_external
{
    int32       va_rawsize;     /* Original data size (includes header) */
    uint32      va_extinfo;     /* External saved size and compression method */
    Oid         va_valueid;     /* Unique ID of value within TOAST table */
    Oid         va_toastrelid;  /* RelID of TOAST table containing it */
} varatt_external;

typedef struct varatt_indirect
{
    struct varlena *pointer;    /* Pointer to in-memory varlena */
} varatt_indirect;

typedef struct varatt_expanded
{
    ExpandedObjectHeader *eohptr;
} varatt_expanded;

/* TOAST vartag enumeration */
typedef enum vartag_external
{
    VARTAG_INDIRECT = 1,
    VARTAG_EXPANDED_RO = 2,
    VARTAG_EXPANDED_RW = 3,
    VARTAG_ONDISK = 18
} vartag_external;

/* Duplicate removed - use the first definition above */

/* VARTAG macros from PostgreSQL */
#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 TrapMacro(true, "unrecognized TOAST vartag"))

/* External TOAST detection macros from PostgreSQL */
#define VARATT_IS_EXTERNAL_ONDISK(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RO)
#define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_EXTERNAL_NON_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))

/* VARATT_EXTERNAL_GET_POINTER macro from PostgreSQL detoast.h - EXACT COPY */
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr) \
do { \
	varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
	assert(VARATT_IS_EXTERNAL(attre)); \
	assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
	memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

/* SET_VARSIZE macros from PostgreSQL - Little-Endian version */
#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len)			SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)

/* TOAST-related constants */
#define VARLENA_EXTSIZE_BITS    30
#define VARLENA_EXTSIZE_MASK    ((1U << VARLENA_EXTSIZE_BITS) - 1)

/* TrapMacro - simplified version */
#define TrapMacro(condition, errorType) (true)

/* TOAST macros */
#define VARTAG_IS_EXPANDED(tag) \
    (((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
    ((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
     VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
     (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
     TrapMacro(true, "unrecognized TOAST vartag"))

#define VARTAG_EXTERNAL(PTR)                VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)               (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR)               VARDATA_1B_E(PTR)

/* PostgreSQL text handling */
#define DatumGetTextPP(X) ((text *) PG_DETOAST_DATUM_PACKED(X))
#define PG_DETOAST_DATUM_PACKED(datum) ((struct varlena *) DatumGetPointer(datum))

/* Fetch attribute value - PostgreSQL standard */
#define fetchatt(A,T) fetch_att(T, (A)->attbyval, (A)->attlen)

/*
 * fetch_att - PostgreSQL standard implementation
 * For systems where Datum is 8 bytes (most modern systems)
 */
#if SIZEOF_DATUM == 8

#define fetch_att(T,attbyval,attlen) \
( \
    (attbyval) ? \
    ( \
        (attlen) == (int) sizeof(Datum) ? \
            *((Datum *)(T)) \
        : \
      ( \
        (attlen) == (int) sizeof(int32) ? \
            Int32GetDatum(*((int32 *)(T))) \
        : \
        ( \
            (attlen) == (int) sizeof(int16) ? \
                Int16GetDatum(*((int16 *)(T))) \
            : \
            ( \
                AssertMacro((attlen) == 1), \
                CharGetDatum(*((char *)(T))) \
            ) \
        ) \
      ) \
    ) \
    : \
    PointerGetDatum((char *) (T)) \
)

#else  /* SIZEOF_DATUM != 8 */

#define fetch_att(T,attbyval,attlen) \
( \
    (attbyval) ? \
    ( \
        (attlen) == (int) sizeof(int32) ? \
            Int32GetDatum(*((int32 *)(T))) \
        : \
        ( \
            (attlen) == (int) sizeof(int16) ? \
                Int16GetDatum(*((int16 *)(T))) \
            : \
            ( \
                AssertMacro((attlen) == 1), \
                CharGetDatum(*((char *)(T))) \
            ) \
        ) \
    ) \
    : \
    PointerGetDatum((char *) (T)) \
)

#endif /* SIZEOF_DATUM == 8 */

#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
    ((attlen) > 0) ? \
    ( \
        (cur_offset) + (attlen) \
    ) \
    : (((attlen) == -1) ? \
    ( \
        (cur_offset) + VARSIZE_ANY(attptr) \
    ) \
    : \
    ( \
        AssertMacro((attlen) == -2), \
        (cur_offset) + (strlen((char *) (attptr)) + 1) \
    )) \
)

/* PostgreSQL-compatible Datum extraction and conversion macros */
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))

/* Note: USE_FLOAT8_BYVAL is typically defined on 64-bit systems */
#ifdef USE_FLOAT8_BYVAL
#define DatumGetInt64(X) ((int64) (X))
#else
#define DatumGetInt64(X) (* ((int64 *) DatumGetPointer(X)))
#endif

#define DatumGetInt32(X) ((int32) (X))
#define DatumGetInt16(X) ((int16) (X))
#define DatumGetBool(X) ((bool) ((X) != 0))

/* Datum creation macros */
#define CharGetDatum(X) ((Datum) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define Int32GetDatum(X) ((Datum) (X))

/* DatumGetFloat4 - PostgreSQL standard */
static inline float4
DatumGetFloat4(Datum X)
{
    union
    {
        int32       value;
        float4      retval;
    }           myunion;

    myunion.value = DatumGetInt32(X);
    return myunion.retval;
}

/* DatumGetFloat8 - PostgreSQL standard */
#ifdef USE_FLOAT8_BYVAL
static inline float8
DatumGetFloat8(Datum X)
{
    union
    {
        int64       value;
        float8      retval;
    }           myunion;

    myunion.value = DatumGetInt64(X);
    return myunion.retval;
}
#else
#define DatumGetFloat8(X) (* ((float8 *) DatumGetPointer(X)))
#endif

/* Text type for variable-length strings */
typedef struct varlena text;

/* PostgreSQL integer limits - from c.h */
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)

/* INT64CONST macro if not defined */
#ifndef INT64CONST
#define INT64CONST(x) (x##LL)
#endif

/* TOAST detoasting - from fmgr.h */
struct varlena *pg_detoast_datum(struct varlena *datum);
#define PG_DETOAST_DATUM(datum) \
	pg_detoast_datum((struct varlena *) DatumGetPointer(datum))

/* Additional PostgreSQL Datum conversion functions - from utils/timestamp.h */
/* PostgreSQL Timestamp is int64, using explicit int64 to avoid conflict with duckdb::Timestamp class */
#define DatumGetTimestamp(X)  ((int64) DatumGetInt64(X))
#define DatumGetTimestampTz(X)	((int64) DatumGetInt64(X))
/* PostgreSQL Interval struct, using :: to avoid conflict with duckdb::Interval class */
#define DatumGetIntervalP(X)  ((::Interval *) DatumGetPointer(X))

/* From utils/date.h */
#define DatumGetDateADT(X)	  ((DateADT) DatumGetInt32(X))
#define DatumGetTimeADT(X)	  ((TimeADT) DatumGetInt64(X))
#define DatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

/* From utils/varbit.h */
#define DatumGetVarBitP(X)		   ((VarBit *) PG_DETOAST_DATUM(X))

/* Additional Datum conversion functions */
#define DatumGetUInt32(X) ((uint32) (X))
#define DatumGetNumeric(X) ((Numeric) PG_DETOAST_DATUM(X))

/* Date/Time constants - from utils/date.h */
#define DATEVAL_NOBEGIN		((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND		((DateADT) PG_INT32_MAX)

/* Timestamp constants - from datatype/timestamp.h */
#define DT_NOBEGIN		PG_INT64_MIN
#define DT_NOEND		PG_INT64_MAX

/* Type OIDs - from catalog/pg_type_d.h */
#ifndef JSONBOID
#define JSONBOID 3802
#endif

/* Additional macros from PostgreSQL needed for varbit_out */
#ifndef BITS_PER_BYTE
#define BITS_PER_BYTE		8
#endif
#ifndef HIGHBIT
#define HIGHBIT					(0x80)
#endif
#ifndef IS_HIGHBIT_SET
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)
#endif

/* VarBit macros from utils/varbit.h */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		bit_len;		/* number of valid bits */
	bits8		bit_dat[FLEXIBLE_ARRAY_MEMBER]; /* bit string, most sig. byte first */
} VarBit;

#define VARBITLEN(PTR)		(((VarBit *) (PTR))->bit_len)
#define VARBITS(PTR)		(((VarBit *) (PTR))->bit_dat)
#define VARBITBYTES(PTR)	(VARSIZE(PTR) - VARHDRSZ - sizeof(int32))

/* Clean definitions - remove conflicting macros */

/* PostgreSQL NUMERIC macros - exact from PostgreSQL source */
#define NUMERIC_HEADER_SIZE(n) \
    (VARHDRSZ + sizeof(uint16) + \
     (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))
#define NUMERIC_NDIGITS(n) \
    ((VARSIZE(n) - NUMERIC_HEADER_SIZE(n)) / sizeof(NumericDigit))
#define NUMERIC_DIGITS(n) \
    (NUMERIC_HEADER_IS_SHORT((n)) ? \
     (n)->choice.n_short.n_data : (n)->choice.n_long.n_data)

/* Additional NUMERIC macros from PostgreSQL */
#define NUMERIC_WEIGHT(n) (NUMERIC_HEADER_IS_SHORT((n)) ? \
    (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
      ~NUMERIC_SHORT_WEIGHT_MASK : 0) | \
     ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
    : ((n)->choice.n_long.n_weight))

#define NUMERIC_DSCALE(n) (NUMERIC_HEADER_IS_SHORT((n)) ? \
    ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT \
    : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))

#define NUMERIC_SIGN(n) \
    (NUMERIC_IS_SHORT(n) ? \
        (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
         NUMERIC_NEG : NUMERIC_POS) : \
        (NUMERIC_IS_SPECIAL(n) ? \
         NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))

/* NUMERIC constants and masks */
#define NUMERIC_SIGN_MASK    0xC000
#define NUMERIC_POS          0x0000
#define NUMERIC_NEG          0x4000
#define NUMERIC_SHORT        0x8000
#define NUMERIC_SPECIAL      0xC000
#define NUMERIC_DSCALE_MASK  0x3FFF
#define NUMERIC_EXT_SIGN_MASK 0xF000
#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)  (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)
#define NUMERIC_EXT_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_SHORT_SIGN_MASK        0x2000
#define NUMERIC_SHORT_DSCALE_MASK      0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT     7
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK      0x003F

/* Basic validity checks */
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)
#define PageIsValid(page) PointerIsValid(page)

/* ItemId macros */
#define ItemIdHasStorage(itemId) \
    ((itemId)->lp_len != 0)
#define ItemIdGetOffset(itemId) ((itemId)->lp_off)
#define ItemIdGetLength(itemId) ((itemId)->lp_len)
#define ItemIdIsNormal(itemId) (((itemId)->lp_flags & 0x03) == 0x01)

/* Page access macros - PostgreSQL standard */
#define PageGetItemId(page, offsetNumber) \
    ((ItemId) (&((PageHeader) (page))->pd_linp[(offsetNumber) - 1]))

#define PageGetItem(page, itemId) \
( \
    AssertMacro(PageIsValid(page)), \
    AssertMacro(ItemIdHasStorage(itemId)), \
    (Item)(((char *)(page)) + ItemIdGetOffset(itemId)) \
)
#define PageGetMaxOffsetNumber(page) \
    (((PageHeader)(page))->pd_lower <= SizeOfPageHeaderData ? 0 : \
     ((((PageHeader)(page))->pd_lower - SizeOfPageHeaderData) / sizeof(ItemIdData)))
#define SizeOfPageHeaderData (offsetof(PageHeaderData, pd_linp))
#define FirstOffsetNumber ((OffsetNumber) 1)

/* Block ID macros */
#define BlockIdSet(blockId, blockNumber) \
( \
    (blockId)->bi_hi = (blockNumber) >> 16, \
    (blockId)->bi_lo = (blockNumber) & 0xffff \
)

/* ItemPointer macros - PostgreSQL standard */
#define ItemPointerSet(pointer, blockNumber, offNum) \
( \
    AssertMacro(PointerIsValid(pointer)), \
    BlockIdSet(&((pointer)->ip_blkid), blockNumber), \
    (pointer)->ip_posid = offNum \
)

/* Datum creation macros - from utils/timestamp.h */
#define TimestampGetDatum(X) Int64GetDatum(X)
#define TimestampTzGetDatum(X) Int64GetDatum(X)
#define IntervalPGetDatum(X) PointerGetDatum(X)

/* From utils/date.h */
#define DateADTGetDatum(X)	  Int32GetDatum(X)
#define TimeADTGetDatum(X)	  Int64GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)

/* Additional conversion macros */
#define NumericGetDatum(X) PointerGetDatum(X)
#define UInt8GetDatum(X) ((Datum) (X))
#define UInt16GetDatum(X) ((Datum) (X))
#define UInt32GetDatum(X) ((Datum) (X))


#endif /* PG_MACROS_H */