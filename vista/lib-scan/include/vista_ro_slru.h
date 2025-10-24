#ifndef __VISTA_RO_SLRU_H__
#define __VISTA_RO_SLRU_H__

#include "vista_heapam_visibility.h"
#include "vista_pg_read_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VISTA_MAX_SLRU_SLOTS 128UL
#define VISTA_SLRU_SLOT_EMPTY -1

#ifndef VISTA_PG_BLCKSZ
#define VISTA_PG_BLCKSZ 8192UL
#endif

struct VistaSlruData
{
    char data[VISTA_PG_BLCKSZ];
} __attribute__((aligned(VISTA_PG_BLCKSZ)));

/* 
 * We do not need to manage shared memory, locks, or control files,
 * because this is read-only SLRU for OLAP engine.
 */
struct VistaSlruCtlData
{
    struct VistaSlruData *page_buffer; /* page buffers */

    int			num_slots;		         /* number of page slots */
    const char *name;			         /* name of this SLRU */
    const char *subdir;			         /* subdirectory in $PGDATA */
    char __pad0[40];

    int *pageno;    /* page number in each slot */
    int *lru_count; /* LRU count for each slot */

    int cur_lru_count;                   /* global LRU counter */
    unsigned int gen;
    bool is_flush;
} __attribute__((aligned(VISTA_PG_BLCKSZ)));
typedef struct VistaSlruCtlData *VistaSlruCtl;

void VistaSlruInit(VistaSlruCtl ctl, const char *name, int num_slots, const char *subdir);
void *VistaSlruReadPage(VistaSlruCtl ctl, int pageno);
void VistaSlruReset(VistaSlruCtl ctl);

#ifdef __cplusplus
}
#endif

#endif /* __VISTA_RO_SLRU_H__ */