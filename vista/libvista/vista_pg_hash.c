/*
 *-------------------------------------------------------------------------
 * vista_hash.c
 *
 *	Functions for computing hash values and searching a hash table
 *	in a remapped VMSeg address space.
 *
 *  Most of the functions are directly ported from PostgreSQL's 
 *  hashfn.c, dynahash.c, and hsearch.h files.
 *
 *-------------------------------------------------------------------------
 */

#include "include/vista_pg_read_buffer.h"
#include <string.h> // For memcmp

#define LIBVISTA_UINT32_ALIGN_MASK (sizeof(uint32_t) - 1)

static inline uint32_t
libvista_pg_rotate_left32(uint32_t word, int n)
{
	return (word << n) | (word >> (32 - n));
}

#define vista_pg_rot(x,k) libvista_pg_rotate_left32(x, k)

/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *
 * If four pairs of (a,b,c) inputs are run through mix(), or through
 * mix() in reverse, there are at least 32 bits of the output that
 * are sometimes the same for one pair and different for another pair.
 * This was tested for:
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * This does not achieve avalanche.  There are input bits of (a,b,c)
 * that fail to affect some output bits of (a,b,c), especially of a.  The
 * most thoroughly mixed value is c, but it doesn't really even achieve
 * avalanche in c.
 *
 * This allows some parallelism.  Read-after-writes are good at doubling
 * the number of bits affected, so the goal of mixing pulls in the opposite
 * direction from the goal of parallelism.  I did what I could.  Rotates
 * seem to cost as much as shifts on every machine I could lay my hands on,
 * and rotates are much kinder to the top and bottom bits, so I used rotates.
 *----------
 */
#define vista_pg_mix(a,b,c) \
{ \
  a -= c;  a ^= vista_pg_rot(c, 4);	c += b; \
  b -= a;  b ^= vista_pg_rot(a, 6);	a += c; \
  c -= b;  c ^= vista_pg_rot(b, 8);	b += a; \
  a -= c;  a ^= vista_pg_rot(c,16);	c += b; \
  b -= a;  b ^= vista_pg_rot(a,19);	a += c; \
  c -= b;  c ^= vista_pg_rot(b, 4);	b += a; \
}

/*----------
 * final -- final mixing of 3 32-bit values (a,b,c) into c
 *
 * Pairs of (a,b,c) values differing in only a few bits will usually
 * produce values of c that look totally different.  This was tested for
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * The use of separate functions for mix() and final() allow for a
 * substantial performance increase since final() does not need to
 * do well in reverse, but is does need to affect all output bits.
 * mix(), on the other hand, does not need to affect all output
 * bits (affecting 32 bits is enough).  The original hash function had
 * a single mixing operation that had to satisfy both sets of requirements
 * and was slower as a result.
 *----------
 */
#define vista_pg_final(a,b,c) \
{ \
  c ^= b; c -= vista_pg_rot(b,14); \
  a ^= c; a -= vista_pg_rot(c,11); \
  b ^= a; b -= vista_pg_rot(a,25); \
  c ^= b; c -= vista_pg_rot(b,16); \
  a ^= c; a -= vista_pg_rot(c, 4); \
  b ^= a; b -= vista_pg_rot(a,14); \
  c ^= b; c -= vista_pg_rot(b,24); \
}

#define VISTA_PG_MOD(x,y)	((x) & ((y)-1))

uint32_t
vista_pg_hash_bytes(const unsigned char *k, int keylen)
{
	uint32_t	a,
				b,
				c,
				len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & LIBVISTA_UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const uint32_t *ka = (const uint32_t *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			vista_pg_mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;

		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32_t) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32_t) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
			a += (k[0] + ((uint32_t) k[1] << 8) + ((uint32_t) k[2] << 16) + ((uint32_t) k[3] << 24));
			b += (k[4] + ((uint32_t) k[5] << 8) + ((uint32_t) k[6] << 16) + ((uint32_t) k[7] << 24));
			c += (k[8] + ((uint32_t) k[9] << 8) + ((uint32_t) k[10] << 16) + ((uint32_t) k[11] << 24));
			vista_pg_mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
		switch (len)
		{
			case 11:
				c += ((uint32_t) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32_t) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32_t) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32_t) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32_t) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32_t) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32_t) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32_t) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32_t) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
	}

	vista_pg_final(a, b, c);

	/* report the result */
	return c;	
}

uint32_t
vista_get_hash_value(const void *keyPtr, vista_pg_Size keysize)
{
	return vista_pg_hash_bytes(
			(const unsigned char *) keyPtr,
			(int) keysize);
}

/*
 * Key (also entry) part of a HASHELEMENT
 */
#define VISTA_PG_ELEMENTKEY(helem)  (((char *)(helem)) + sizeof(VISTA_PG_HASHELEMENT))

/*
 * Translate a pointer from the original VMSeg address space to the current
 * process's remapped address space.
 */
static inline void *
translate_ptr(void *original_ptr, void *remapped_base, void *original_base)
{
    if (original_ptr == NULL)
	{
		return NULL;
	}

	ptrdiff_t offset = (char *)original_ptr - (char *)original_base;
    return (void *)((char *)remapped_base + offset);
}

/*
 * Convert a hash value to a bucket number.
 * (Ported from dynahash.c's calc_bucket)
 */
static inline uint32_t
vista_pg_calc_bucket(struct VISTA_PG_HASHHDR *hctl, uint32_t hash_val)
{
	uint32_t bucket;

	bucket = hash_val & hctl->high_mask;
	if (bucket > hctl->max_bucket)
		bucket = bucket & hctl->low_mask;

	return bucket;
}


void *
vista_hash_search(vista_reconstructed_buffer_pool *pool,
				  const void *keyPtr,
				  uint32_t hashvalue)
{
    struct VISTA_PG_HASHHDR *hctl = pool->hash_header;
	uint32_t		bucket;
	long		segment_num;
	long		segment_ndx;
	VISTA_PG_HASHSEGMENT segp;
	VISTA_PG_HASHBUCKET	currBucket;
	void*	remapped_vmseg_base;
	void*	original_vmseg_base;

	remapped_vmseg_base = (void *) pool->remapped_vmseg_base_addr;
	original_vmseg_base = (void *) pool->original_vmseg_base_addr;

    /*
	 * Do the initial lookup
	 */
	bucket = vista_pg_calc_bucket(hctl, hashvalue);

	segment_num = bucket >> pool->sshift;
	segment_ndx = VISTA_PG_MOD(bucket, pool->ssize);

    // Get the segment pointer (this is a pointer *within* the directory array)
    // and translate it.
	segp = (VISTA_PG_HASHSEGMENT) translate_ptr(pool->hash_directory[segment_num],
                                      remapped_vmseg_base,
                                      original_vmseg_base);

    vista_pg_compiler_barrier();

    if (segp == NULL)
        return NULL; // Should not happen in a properly initialized table

    // Get the head of the bucket's collision chain and translate it.
    currBucket = (VISTA_PG_HASHBUCKET)translate_ptr(segp[segment_ndx],
                                           remapped_vmseg_base,
                                           original_vmseg_base);
   
    vista_pg_compiler_barrier();

    /*
	 * Follow collision chain looking for matching key
	 */
	while (currBucket != NULL)
	{
		if (currBucket->hashvalue == hashvalue &&
			memcmp(VISTA_PG_ELEMENTKEY(currBucket), keyPtr, pool->keysize) == 0)
        {
			return (void *) VISTA_PG_ELEMENTKEY(currBucket);
        }

        // Get the next link in the chain and translate it for the next iteration.
		currBucket = (VISTA_PG_HASHBUCKET)translate_ptr(currBucket->link,
                                               remapped_vmseg_base,
                                               original_vmseg_base);
	    vista_pg_compiler_barrier();
    }

    return NULL; // Not found
}
