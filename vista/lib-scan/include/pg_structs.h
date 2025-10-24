#ifndef PG_STRUCTS_H
#define PG_STRUCTS_H

#include "pg_types.h"
#include "pg_constants.h"

/* Name data structure */
typedef struct nameData
{
  char data[NAMEDATALEN];
} NameData;
typedef NameData *Name;

/* PostgreSQL varlena structure for variable-length data */
struct varlena {
    char vl_len_[4];  /* 4-byte length word */
    char vl_dat[FLEXIBLE_ARRAY_MEMBER];  /* data starts here */
};

/* Block ID and Item Pointer */
typedef struct BlockIdData
{
  uint16 bi_hi;
  uint16 bi_lo;
} BlockIdData;

typedef struct ItemPointerData
{
  BlockIdData ip_blkid;
  OffsetNumber ip_posid;
} ItemPointerData;

/* pg_attribute catalog structure - PostgreSQL REL_15_2 compatible */
#define CATALOG(name, oid, oidmacro) typedef struct CppConcat(FormData_, name)
CATALOG(pg_attribute, 1249, AttributeRelationId) BKI_BOOTSTRAP BKI_ROWTYPE_OID(75, AttributeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
  Oid attrelid BKI_LOOKUP(pg_class);
  NameData attname;
  Oid atttypid BKI_LOOKUP(pg_type);
  int32 attstattarget BKI_DEFAULT(-1);
  int16 attlen;
  int16 attnum;
  int32 attndims;
  int32 attcacheoff BKI_DEFAULT(-1);
  int32 atttypmod BKI_DEFAULT(-1);
  bool attbyval;
  char attalign;
  char attstorage;
  char attcompression BKI_DEFAULT('\0');
  bool attnotnull;
  bool atthasdef BKI_DEFAULT(f);
  bool atthasmissing BKI_DEFAULT(f);
  char attidentity BKI_DEFAULT('\0');
  char attgenerated BKI_DEFAULT('\0');
  bool attisdropped BKI_DEFAULT(f);
  bool attislocal BKI_DEFAULT(t);
  int32 attinhcount BKI_DEFAULT(0);
  Oid attcollation BKI_LOOKUP_OPT(pg_collation);
#ifdef CATALOG_VARLEN
  aclitem attacl[1] BKI_DEFAULT(_null_);
  text attoptions[1] BKI_DEFAULT(_null_);
  text attfdwoptions[1] BKI_DEFAULT(_null_);
  anyarray attmissingval BKI_DEFAULT(_null_);
#endif 
} FormData_pg_attribute;

typedef FormData_pg_attribute *Form_pg_attribute;

/* Tuple constraint structures */
typedef struct AttrDefault
{
  AttrNumber adnum;
  char *adbin;
} AttrDefault;

typedef struct ConstrCheck
{
  char *ccname;
  char *ccbin;
  bool ccvalid;
  bool ccnoinherit;
} ConstrCheck;

typedef struct TupleConstr
{
  AttrDefault *defval;
  ConstrCheck *check;
  struct AttrMissing *missing;
  uint16 num_defval;
  uint16 num_check;
  bool has_not_null;
  bool has_generated_stored;
} TupleConstr;

typedef struct TupleDescData
{
  int natts;
  Oid tdtypeid;
  int32 tdtypmod;
  int tdrefcount;
  TupleConstr *constr;
  FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
} TupleDescData;

typedef TupleDescData *TupleDesc;

/* Page structures */
typedef struct ItemIdData
{
  unsigned lp_off:15,
           lp_flags:2,
           lp_len:15;
} ItemIdData;

typedef ItemIdData *ItemId;

typedef struct
{
  uint32 xlogid;
  uint32 xrecoff;
} PageXLogRecPtr;

typedef struct PageHeaderData
{
  PageXLogRecPtr pd_lsn;
  uint16 pd_checksum;
  uint16 pd_flags;
  LocationIndex pd_lower;
  LocationIndex pd_upper;
  LocationIndex pd_special;
  uint16 pd_pagesize_version;
  TransactionId pd_prune_xid;
  ItemIdData pd_linp[FLEXIBLE_ARRAY_MEMBER];
} PageHeaderData;

typedef PageHeaderData *PageHeader;

/* Tuple table slot structures */
struct TupleTableSlotOps;
typedef struct TupleTableSlotOps TupleTableSlotOps;

typedef struct TupleTableSlot
{
  NodeTag type;
#define FIELDNO_TUPLETABLESLOT_FLAGS 1
  uint16 tts_flags;
#define FIELDNO_TUPLETABLESLOT_NVALID 2
  AttrNumber tts_nvalid;
  const TupleTableSlotOps *const tts_ops;
#define FIELDNO_TUPLETABLESLOT_TUPLEDESCRIPTOR 4
  TupleDesc tts_tupleDescriptor;
#define FIELDNO_TUPLETABLESLOT_VALUES 5
  Datum *tts_values;
#define FIELDNO_TUPLETABLESLOT_ISNULL 6
  bool *tts_isnull;
  void *tts_mcxt;
  ItemPointerData tts_tid;
  Oid tts_tableOid;
} TupleTableSlot;

/* Heap tuple structures */
typedef struct HeapTupleFields
{
  TransactionId t_xmin;
  TransactionId t_xmax;

  union
  {
    CommandId t_cid;
    TransactionId t_xvac;
  } t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
  int32 datum_len_;
  int32 datum_typmod;
  Oid datum_typeid;
} DatumTupleFields;

struct HeapTupleHeaderData
{
  union
  {
    HeapTupleFields t_heap;
    DatumTupleFields t_datum;
  } t_choice;
  ItemPointerData t_ctid;
#define FIELDNO_HEAPTUPLEHEADERDATA_INFORMASK2 2
  uint16 t_infomask2;
#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
  uint16 t_infomask;
#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
  uint8 t_hoff;
#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
  bits8 t_bits[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct HeapTupleHeaderData HeapTupleHeaderData;
typedef HeapTupleHeaderData *HeapTupleHeader;

typedef struct HeapTupleData
{
  uint32 t_len;
  ItemPointerData t_self;
  Oid t_tableOid;
#define FIELDNO_HEAPTUPLEDATA_DATA 3
  HeapTupleHeader t_data;
} HeapTupleData;

typedef HeapTupleData *HeapTuple;

typedef struct HeapTupleTableSlot
{
  pg_node_attr(abstract)
  TupleTableSlot base;
#define FIELDNO_HEAPTUPLETABLESLOT_TUPLE 1
  HeapTuple tuple;
#define FIELDNO_HEAPTUPLETABLESLOT_OFF 2
  uint32 off;
  HeapTupleData tupdata;
} HeapTupleTableSlot;

typedef struct BufferHeapTupleTableSlot
{
  pg_node_attr(abstract)
  HeapTupleTableSlot base;
  Buffer buffer;
} BufferHeapTupleTableSlot;

/* Thread argument structure */
struct args_struct {
  int arg1;
  int arg2;
  int arg3;
  int arg4;
};

/* PostgreSQL NUMERIC structure */
struct NumericShort {
    uint16      n_header;           /* Sign + display scale + weight */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong {
    uint16      n_sign_dscale;      /* Sign + display scale */
    int16       n_weight;           /* Weight of 1st digit  */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice {
    uint16              n_header;       /* Header word */
    struct NumericLong  n_long;        /* Long form (4-byte header) */
    struct NumericShort n_short;       /* Short form (2-byte header) */
};

struct NumericData {
    int32           vl_len_;        /* varlena header (do not touch directly!) */
    union NumericChoice choice;
};

#endif /* PG_STRUCTS_H */