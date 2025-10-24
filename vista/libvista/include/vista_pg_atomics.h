#ifndef __VISTA_ATOMICS_H__
#define __VISTA_ATOMICS_H__

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#define PG_INT_MAX 2147483647
#define PG_INT_MIN (-PG_INT_MAX - 1)

#define vista_pg_compiler_barrier() __asm__ __volatile__("" ::: "memory")

/*
 * This is a direct port of the atomic operations from PostgreSQL's
 * src/include/port/atomics/arch-x86.h and generic headers,
 * ensuring the same semantics and performance characteristics.
 */

typedef struct vista_atomic_uint32
{
	volatile uint32_t value;
} vista_atomic_uint32;

/*
 * vista_atomic_flag
 *
 * This is a direct port of PostgreSQL's pg_atomic_flag from
 * src/include/port/atomics/arch-x86.h.
 */
typedef struct vista_atomic_flag
{
	volatile char value;
} vista_atomic_flag;


static inline void
vista_atomic_init_flag(volatile vista_atomic_flag *ptr)
{
    __asm__ __volatile__("" ::: "memory");
    ptr->value = 0;
}

/*
 * Returns true if the flag was successfully set (i.e., was not set before).
 * Returns false if the flag was already set.
 */
static inline bool
vista_atomic_test_and_set_flag(volatile vista_atomic_flag *ptr)
{
	register char _res = 1;

	__asm__ __volatile__(
		"\tlock\n"
		"\txchgb\t%0,%1\n"
:        "+q"(_res), "+m"(ptr->value)
: 
:        "memory");
	return _res == 0;
}

static inline void
vista_atomic_clear_flag(volatile vista_atomic_flag *ptr)
{
	__asm__ __volatile__("" ::: "memory");
	ptr->value = 0;
}

static inline void
vista_atomic_init_u32(volatile vista_atomic_uint32 *ptr, uint32_t val)
{
	ptr->value = val;
}

/*
 * Atomic read but NO BARRIER SEMANTICS - You may want to use 
 * a memory barrier AFTER this read to ensure that
 * any instructions after this read are actually executed after this.
 * (Especially in case you want to use the value as a flag/state/etc.)
 */
static inline uint32_t
vista_atomic_read_u32(volatile vista_atomic_uint32 *ptr)
{
	return ptr->value;
}

/*
 * Atomic write but NO BARRIER SEMANTICS - You may want to use
 * a memory barrier BEFORE this write to ensure that
 * any instruction before this write are actually executed before this.
 * (Especially in case you want to use the value as a flag/state/etc.)
 */
static inline void
vista_atomic_write_u32(volatile vista_atomic_uint32 *ptr, uint32_t val)
{
	ptr->value = val;
}

static inline uint32_t
vista_atomic_add_fetch_u32(volatile vista_atomic_uint32 *ptr, int32_t add_)
{
	// The original pg_atomic_add_fetch_u32 returns the *new* value.
	// xadd returns the *old* value, so we must add the increment to it,
	// so that it becomes add-and-fetch.
	uint32_t res;
	__asm__ __volatile__(
		"\tlock\t\n"
		"\txaddl\t%0,%1\t\n"
:        "=q"(res), "=m"(ptr->value)
:        "0" (add_), "m"(ptr->value)
:        "memory", "cc");
	return res + add_;
}

static inline uint32_t
vista_atomic_sub_fetch_u32(volatile vista_atomic_uint32 *ptr, int32_t sub_)
{
	assert(sub_ != PG_INT_MIN);
    return __sync_fetch_and_sub(&ptr->value, sub_) - sub_;
}

/* 
 * Atomically compare the current value of ptr with *expected and store newval
 * iff ptr and *expected have the same value. The current value of *ptr will
 * always be stored in *expected.
 * Return true if values have been exchanged, false otherwise
 */
static inline bool
vista_atomic_compare_exchange_u32(volatile vista_atomic_uint32 *ptr,
							   uint32_t *expected, uint32_t newval)
{
	char	ret;
	__asm__ __volatile__(
		"\tlock\t\n"
		"\tcmpxchgl\t%4,%5\t\n"
		"\tsetz\t\t%2\t\n"
:        "=a" (*expected), "=m"(ptr->value), "=q" (ret)
:        "a" (*expected), "r" (newval), "m"(ptr->value)
:        "memory", "cc");
	return (bool) ret;
}

#endif /* __VISTA_ATOMICS_H__ */
