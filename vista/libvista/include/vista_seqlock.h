#ifndef __VISTA_SEQLOCK_H__
#define __VISTA_SEQLOCK_H__

#include "vista_pg_atomics.h"

/*
 * This is a direct port of the seqlock implementation from PostgreSQL's
 * src/include/storage/vista_seqlock.h, adapted to use the portable
 * atomics implemented in vista_atomics.h.
 */

typedef vista_atomic_uint32 libvista_seqlock_t;

static inline void
libvista_seqlock_init(libvista_seqlock_t *s)
{
    vista_atomic_init_u32(s, 0);
}

/*
 * Full barrier semantics. Assumed to occur very rarely. 
 */
static inline void
libvista_seqlock_write_begin(libvista_seqlock_t *s)
{
    vista_atomic_add_fetch_u32(s, 1);
}

/*
 * Full barrier semantics. Assumed to occur very rarely. 
 */
static inline void
libvista_seqlock_write_end(libvista_seqlock_t *s)
{
    vista_atomic_add_fetch_u32(s, 1);
}

/*
 * Retry until read even-numbered seqlock value. 
 * Return the seqlock value.
 */
static inline uint32_t
libvista_seqlock_read_begin(libvista_seqlock_t *s)
{
    uint32_t v;
    do
    {
        v = vista_atomic_read_u32((vista_atomic_uint32 *) s);
    } while (v & 1);

    /*
     * Make sure that other memory accesses afterwards are executed after 
     * reading the even number `v`
     */
    vista_pg_compiler_barrier();
    return v;
}

/*
 * Check the current seqlock value to know whether it is safe.
 * Return True if the value has changed(need to retry)
 */
static inline bool
libvista_seqlock_read_retry(libvista_seqlock_t *s, uint32_t startv)
{
    /*
     * Make sure that other memory accesses beforehand are executed before 
     * reading the even number `v`
     */
    uint32_t v2;
    vista_pg_compiler_barrier();
    v2 = vista_atomic_read_u32((vista_atomic_uint32 *) s);
    return (v2 != startv);
}

#endif /* __VISTA_SEQLOCK_H__ */
