#ifndef PG_CONSTANTS_H
#define PG_CONSTANTS_H

/* Block and segment size constants */
#define BLOCK_SIZE 8192       /* The size of the block */
#define RELSEG_SIZE 131072    /* The size of the each segment */
#define MAX_SEGMENT_COUNT 100 /* the number of maximum segment per table */
#define MAX_TABLES 50 /* maximum number of tables in concurrent access */

/* Name length constant */
#define NAMEDATALEN 64

/* Flexible array member */
#define FLEXIBLE_ARRAY_MEMBER /* empty */

/* Buffer constants */
#define InvalidBuffer 0

/* TTS flags */
#define TTS_FLAG_EMPTY (1 << 1)
#define TTS_FLAG_SLOW (1 << 3)
#define TTS_FLAG_FIXED (1 << 4)

/* TTS flag check macros */
#define TTS_FIXED(slot) (((slot)->tts_flags & TTS_FLAG_FIXED) != 0)

/* Alignment values */
#define ALIGNOF_SHORT 2
#define ALIGNOF_INT 4
#define ALIGNOF_LONG 8
#define ALIGNOF_DOUBLE 8
#define MAXIMUM_ALIGNOF 8

/* Size of Datum on this platform */
#ifndef SIZEOF_DATUM
#define SIZEOF_DATUM 8
#endif

/* Float8 pass-by-value flag - typically true on 64-bit systems */
#ifndef USE_FLOAT8_BYVAL
#define USE_FLOAT8_BYVAL 1
#endif

/* BKI macros */
#define BKI_LOOKUP(catalog)
#define BKI_LOOKUP_OPT(catalog)
#define BKI_BOOTSTRAP
#define BKI_SHARED_RELATION
#define BKI_ROWTYPE_OID(oid, oidmacro)
#define BKI_SCHEMA_MACRO
#define BKI_DEFAULT(value)

/* VARLENA constants */
#define VARHDRSZ ((int32) sizeof(int32))

/* Time constants - PostgreSQL compatible */
#define UNIX_EPOCH_JDATE     2440588  /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE 2451545  /* == date2j(2000, 1, 1) */
#define SECS_PER_DAY         86400
#define USECS_PER_DAY        INT64CONST(86400000000)
#define USECS_PER_HOUR       INT64CONST(3600000000)
#define USECS_PER_MINUTE     INT64CONST(60000000)
#define USECS_PER_SEC        INT64CONST(1000000)

/* Misc macros */
#define CppConcat(x, y) x##y
#define pg_node_attr(...)

/* PostgreSQL Type OIDs */
#define BOOLOID         16
#define BYTEAOID        17
#define INT8OID         20
#define INT2OID         21
#define INT4OID         23
#define TEXTOID         25
#define FLOAT4OID       700
#define FLOAT8OID       701
#define BPCHAROID       1042    /* CHAR(n) - blank-padded char */
#define VARCHAROID      1043
#define TIMESTAMPOID    1114
#define TIMESTAMPTZOID  1184
#define NUMERICOID      1700

/* Heap tuple header infomask bits - PostgreSQL standard */
#define HEAP_HASNULL            0x0001  /* has null attribute(s) */
#define HEAP_HASVARWIDTH        0x0002  /* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL        0x0004  /* has external stored attribute(s) */
#define HEAP_HASOID_OLD         0x0008  /* has an object-id field */
#define HEAP_XMAX_KEYSHR_LOCK   0x0010  /* xmax is a key-shared locker */
#define HEAP_COMBOCID           0x0020  /* t_cid is a combo CID */
#define HEAP_XMAX_EXCL_LOCK     0x0040  /* xmax is exclusive locker */
#define HEAP_XMAX_LOCK_ONLY     0x0080  /* xmax, if valid, is only a locker */

/* infomask2 bits */
#define HEAP_NATTS_MASK         0x07FF  /* 11 bits for number of attributes */

#endif /* PG_CONSTANTS_H */