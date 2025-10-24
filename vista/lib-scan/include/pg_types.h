#ifndef PG_TYPES_H
#define PG_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

/* Basic type definitions */
typedef signed int int32;        /* == 32 bits */
typedef signed short int16;      /* == 16 bits */
typedef unsigned short uint16;
typedef unsigned int uint32;
typedef uint8_t uint8;
typedef uint64_t uint64;
typedef int64_t int64;

/* PostgreSQL floating point types */
typedef float float4;
typedef double float8;

/* Pointer type */
typedef char *Pointer;

/* Item type for page items */
typedef Pointer Item;

typedef unsigned int Oid;
typedef uintptr_t Datum;
typedef uint16 LocationIndex;
typedef uint16 OffsetNumber;
typedef uint32 TransactionId;
typedef uint16_t NodeTag;
typedef uint16_t AttrNumber;

typedef uint32 BlockNumber;
typedef size_t Size;
typedef int File;

typedef int Buffer;
typedef uint8_t bits8;

typedef uint32 CommandId;

/* Time-related types */
typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeOffset;
typedef int32 fsec_t;         /* fractional seconds (in microseconds) */
typedef int32 DateADT;
typedef int64 TimeADT;

/* TimeTzADT structure from PostgreSQL */
typedef struct
{
	TimeADT		time;			/* all time units other than months and years */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT;

/* PostgreSQL Interval structure */
typedef struct
{
	TimeOffset	time;			/* all time units other than months and years */
	int32		day;			/* days, after time for alignment */
	int32		month;			/* months and years, after time for alignment */
} Interval;

/* Postgres pg_tm struct */
struct pg_tm {
    int tm_sec;
    int tm_min;
    int tm_hour;
    int tm_mday;
    int tm_mon;         
    int tm_year;        
    int tm_wday;
    int tm_yday;
    int tm_isdst;
    long int tm_gmtoff;
    const char *tm_zone;
};

/* NUMERIC types */
typedef int16 NumericDigit;

/* Forward declaration for NUMERIC structure */
typedef struct NumericData *Numeric;

/* NUMERIC constants */
#define NBASE       10000
#define DEC_DIGITS  4


/* INT64CONST macro */
#ifndef INT64CONST
#define INT64CONST(x) ((int64) x##LL)
#endif

#endif /* PG_TYPES_H */
