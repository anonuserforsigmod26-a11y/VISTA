#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "pg_types.h"
#include "pg_constants.h"
#include "pg_macros.h"
#include "pg_structs.h"
#include "tuple_access.h"
#include "memory_utils.h"

/* --------------------------------
 *      ExecStoreBufferHeapTuple
 *
 *      This function is used to store an on-disk physical tuple from a buffer
 *      into a specified slot in the tuple table.
 *
 *      tuple:  tuple to store
 *      slot:   TTSOpsBufferHeapTuple type slot to store it in
 *      buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *
 * The tuple table code acquires a pin on the buffer which is held until the
 * slot is cleared, so that the tuple won't go away on us.
 *
 * Return value is just the passed-in slot pointer.
 *
 * If the target slot is not guaranteed to be TTSOpsBufferHeapTuple type slot,
 * use the, more expensive, ExecForceStoreHeapTuple().
 * --------------------------------
 */
TupleTableSlot *ExecStoreBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot,
                                        Buffer buffer) {
  assert(tuple != NULL);
  assert(slot != NULL);
  assert(slot->tts_tupleDescriptor != NULL);

  BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *)slot;

  slot->tts_flags &= ~TTS_FLAG_EMPTY;
  slot->tts_nvalid = 0;
  bslot->base.tuple = tuple;
  bslot->base.off = 0;
  slot->tts_tid = tuple->t_self;
  slot->tts_tableOid = tuple->t_tableOid;

  return slot;
}

/*
 * slot_deform_heap_tuple
 *      Given a TupleTableSlot, extract data from the slot's physical tuple
 *      into its Datum/isnull arrays.  Data is extracted up through the
 *      natts'th column (caller must ensure this is a legal column number).
 *
 *      This is essentially an incremental version of heap_deform_tuple:
 *      on each call we extract attributes up to the one needed, without
 *      re-computing information about previously extracted attributes.
 *      slot->tts_nvalid is the number of attributes already extracted.
 *
 * This is marked as always inline, so the different offp for different types
 * of slots gets optimized away.
 */
void slot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
                           int natts) {
  TupleDesc tupleDesc = slot->tts_tupleDescriptor;
  Datum *values = slot->tts_values;
  bool *isnull = slot->tts_isnull;
  HeapTupleHeader tup = tuple->t_data;
  bool hasnulls = HeapTupleHasNulls(tuple);
  int attnum;
  char *tp;                /* ptr to tuple data */
  uint32 off;              /* offset in tuple data */
  bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
  bool slow;               /* can we use/set attcacheoff? */

  /* We can only fetch as many attributes as the tuple has. */
  natts = Min(HeapTupleHeaderGetNatts(tuple->t_data), natts);

  /*
   * Check whether the first call for this tuple, and initialize or restore
   * loop state.
   */
  attnum = slot->tts_nvalid;
  if (attnum == 0) {
    /* Start from the first attribute */
    off = 0;
    slow = false;
  } else {
    /* Restore state from previous execution */
    off = *offp;
    slow = TTS_SLOW(slot);
  }

  tp = (char *)tup + tup->t_hoff;

  for (; attnum < natts; attnum++) {
    Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

    if (hasnulls && att_isnull(attnum, bp)) {
      values[attnum] = (Datum)0;
      isnull[attnum] = true;
      slow = true; /* can't use attcacheoff anymore */
      continue;
    }

    isnull[attnum] = false;

    if (!slow && thisatt->attcacheoff >= 0)
      off = thisatt->attcacheoff;
    else if (thisatt->attlen == -1) {
      /*
       * We can only cache the offset for a varlena attribute if the
       * offset is already suitably aligned, so that there would be no
       * pad bytes in any case: then the offset will be valid for either
       * an aligned or unaligned value.
       */
      if (!slow && off == att_align_nominal(off, thisatt->attalign))
        thisatt->attcacheoff = off;
      else {
        off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
        slow = true;
      }
    } else {
      /* not varlena, so safe to use att_align_nominal */
      off = att_align_nominal(off, thisatt->attalign);

      if (!slow) thisatt->attcacheoff = off;
    }

    values[attnum] = fetchatt(thisatt, tp + off);

    off = att_addlength_pointer(off, thisatt->attlen, tp + off);

    if (thisatt->attlen <= 0) slow = true; /* can't use attcacheoff anymore */
  }

  /*
   * Save state for next execution
   */
  slot->tts_nvalid = attnum;
  *offp = off;
  if (slow)
    slot->tts_flags |= TTS_FLAG_SLOW;
  else
    slot->tts_flags &= ~TTS_FLAG_SLOW;
}

/* --------------------------------
 *		MakeTupleTableSlot
 *
 *		Basic routine to make an empty TupleTableSlot of given
 *		TupleTableSlotType. If tupleDesc is specified the slot's
 *descriptor is fixed for its lifetime, gaining some efficiency. If that's
 *		undesirable, pass NULL.
 * --------------------------------
 */
TupleTableSlot *MakeTupleTableSlot(TupleDesc tupleDesc) {
  Size basesz, allocsz;
  TupleTableSlot *slot;

  // basesz = tts_ops->base_slot_size;
  basesz = 112; /* Size of BufferHeapTuple */

  /*
   * When a fixed descriptor is specified, we can reduce overhead by
   * allocating the entire slot in one go.
   */
  if (tupleDesc)
    allocsz = MAXALIGN(basesz) + MAXALIGN(tupleDesc->natts * sizeof(Datum)) +
              MAXALIGN(tupleDesc->natts * sizeof(bool));
  else
    allocsz = basesz;

  slot = (TupleTableSlot*)simple_malloc(allocsz);
  memset(slot, 0, allocsz);  /* Initialize all fields to 0 */
  /* const for optimization purposes, OK to modify at allocation time */
  // *((const TupleTableSlotOps **)&slot->tts_ops) = tts_ops;
  // slot->type = T_TupleTableSlot;
  slot->type = 112;
  slot->tts_flags |= TTS_FLAG_EMPTY;
  if (tupleDesc != NULL) 
    slot->tts_flags |= TTS_FLAG_FIXED;
  slot->tts_tupleDescriptor = tupleDesc;
  // slot->tts_mcxt = CurrentMemoryContext;
  slot->tts_nvalid = 0;

  if (tupleDesc != NULL) {
    slot->tts_values = (Datum *)(((char *)slot) + MAXALIGN(basesz));
    slot->tts_isnull = (bool *)(((char *)slot) + MAXALIGN(basesz) +
                                MAXALIGN(tupleDesc->natts * sizeof(Datum)));

    // PinTupleDesc(tupleDesc);
  }

  /*
   * And allow slot type specific initialization.
   */
  // slot->tts_ops->init(slot);iterm

  return slot;
}

/* --------------------------------
 *      ExecClearTuple
 *
 *      Clear the slot's contents - PostgreSQL compatible implementation
 * --------------------------------
 */
void ExecClearTuple(TupleTableSlot *slot) {
    if (!slot) return;
    
    /* Clear tuple reference */
    BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *)slot;
    bslot->base.tuple = NULL;
    bslot->base.off = 0;
    
    /* Reset slot state */
    slot->tts_nvalid = 0;
    slot->tts_flags |= TTS_FLAG_EMPTY;
    
    /* Clear TID */
    ItemPointerSet(&slot->tts_tid, 0, 0);
}

/* --------------------------------
 *      ExecDropSingleTupleTableSlot
 *
 *      Release a TupleTableSlot - PostgreSQL compatible implementation
 *      DON'T use this on a slot that's part of a tuple table list!
 * --------------------------------
 */
void ExecDropSingleTupleTableSlot(TupleTableSlot *slot) {
    if (!slot) return;
    
    /* Clear tuple data first */
    ExecClearTuple(slot);
    
    /* Release tuple descriptor reference if needed */
    if (slot->tts_tupleDescriptor) {
        /* In full PostgreSQL this would call ReleaseTupleDesc */
        /* For our simplified version, we don't own the descriptor */
    }
    
    /* In our implementation, we always use fixed allocation:
     * tts_values and tts_isnull are allocated as part of the slot memory block,
     * so they don't need separate free() calls */
    
    /* Free the slot itself */
    simple_free(slot);
}