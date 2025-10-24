// #ifndef VISTA_SEQLOCK_H
// #define VISTA_SEQLOCK_H

/* Seqlock for vista buffer manager. */

#include "c.h"
#include "port/atomics.h"
/*------------------------------------------------------------------------
 * vista_seqlock_t
 *
 *   A 32-bit counter.  Even values mean “no writer active”; an odd
 *   value means “writer in progress.”  Readers sample it before/after
 *   reading protected fields to detect torn updates.
 *------------------------------------------------------------------------
 */
typedef pg_atomic_uint32 vista_seqlock_t;

/*------------------------------------------------------------------------
 * vista_seqlock_init
 *   Initialize to 0 (no writer).
 *------------------------------------------------------------------------
 */
static inline void
vista_seqlock_init(vista_seqlock_t *s)
{
    pg_atomic_init_u32(s, 0);
}

/*------------------------------------------------------------------------
 * vista_seqlock_write_begin
 *   Begin a write phase: bump to odd (acquire semantics).
 *------------------------------------------------------------------------
 */
static inline void
vista_seqlock_write_begin(vista_seqlock_t *s)
{
    /* +1 flips even→odd; acquire ordering so later writes aren’t reordered before */
    pg_atomic_add_fetch_u32(s, 1);
}
/*------------------------------------------------------------------------
 * vista_seqlock_write_end
 *   End a write phase: bump to even (release semantics).
 *------------------------------------------------------------------------
 */
static inline void
vista_seqlock_write_end(vista_seqlock_t *s)
{
    /* +1 flips odd→even; release ordering so earlier writes aren’t reordered after */
    pg_atomic_add_fetch_u32(s, 1);
}

/*------------------------------------------------------------------------
 * vista_seqlock_read_begin
 *   Loop until seqlock is even, then return that value.
 *------------------------------------------------------------------------
 */
static inline uint32
vista_seqlock_read_begin(vista_seqlock_t *s)
{
    uint32 v;
    do
    {
        v = pg_atomic_read_u32((pg_atomic_uint32 *)s);
        /* if odd → a writer is in the middle of an update, retry */
    } while (v & 1);
    return v;
}

/*------------------------------------------------------------------------
 * vista_seqlock_read_retry
 *   Return true if seqlock has changed since 'startv' (or become odd).
 *   Call this after reading your fields to detect torn updates.
 *------------------------------------------------------------------------
 */
static inline bool
vista_seqlock_read_retry(vista_seqlock_t *s, uint32 startv)
{
    uint32 v2 = pg_atomic_read_u32((pg_atomic_uint32 *)s);
    return (v2 != startv);
}


// #endif                            /* VISTA_SEQLOCK_H */