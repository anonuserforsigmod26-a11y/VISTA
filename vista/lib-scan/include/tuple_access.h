#ifndef TUPLE_ACCESS_H
#define TUPLE_ACCESS_H

#include "pg_types.h"
#include "pg_structs.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Tuple table slot operations */
TupleTableSlot *ExecStoreBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot,
                                        Buffer buffer);
void slot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
                           int natts);
TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc);
void ExecClearTuple(TupleTableSlot *slot);
void ExecDropSingleTupleTableSlot(TupleTableSlot *slot);

#ifdef __cplusplus
}
#endif

#endif /* TUPLE_ACCESS_H */