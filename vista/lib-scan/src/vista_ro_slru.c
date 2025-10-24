/*
 * VISTA's read-only simple LRU implementation.
 */
#include "vista_ro_slru.h"
#include "vista_remapped_buffer_access.h"

#include <stdio.h>
#include <unistd.h>

void VistaSlruInit(VistaSlruCtl ctl, const char *name, int num_slots, const char *subdir)
{
    ctl->num_slots = num_slots;
    ctl->name = name;
    ctl->subdir = subdir;

    ctl->page_buffer = (struct VistaSlruData *)malloc(sizeof(struct VistaSlruData) * num_slots);
    ctl->pageno = (int *)malloc(sizeof(int) * num_slots);
    ctl->lru_count = (int *)malloc(sizeof(int) * num_slots);
    if (!ctl->page_buffer || !ctl->pageno || !ctl->lru_count) {
        fprintf(stderr, "[lib-scan] SLRU init memory allocation failed\n");
        exit(-1);
    }

    for (int i = 0; i < num_slots; i++) {
        ctl->pageno[i] = VISTA_SLRU_SLOT_EMPTY;
        ctl->lru_count[i] = 0;
    }
    ctl->cur_lru_count = 0;
}

static inline int VistaSlruFindSlot(VistaSlruCtl ctl, int pageno)
{
    int i;
    for (i = 0; i < ctl->num_slots; i++) {
        if (ctl->pageno[i] == pageno) {
            return i;
        }
    }
    return -1;
}

static void *VistaSlruLoadPage(VistaSlruCtl ctl, int slotno, int pageno)
{
    char path[VISTA_MAXPGPATH];
    int segno = pageno / VISTA_SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % VISTA_SLRU_PAGES_PER_SEGMENT;
    off_t offset = rpageno * VISTA_PG_BLCKSZ;
    int fd;

    snprintf(path, VISTA_MAXPGPATH, "%s/%s/%04X", VISTA_PG_PATH, ctl->subdir, segno);
    if ((fd = open(path, O_RDONLY, (0400|0200))) < 0) {
        fprintf(stderr, "[lib-scan] SLRU open file error: %s\n", path);
        return NULL;
    }

    if (pread(fd, ctl->page_buffer[slotno].data, VISTA_PG_BLCKSZ, offset) != VISTA_PG_BLCKSZ) {
        close(fd);
        fprintf(stderr, "[lib-scan] SLRU read file error(pread): %s\n", path);
        return NULL;
    }

    close(fd);
    return (void *)ctl->page_buffer[slotno].data;
}

static void VistaSlruInvalidateIfNeeded(VistaSlruCtl ctl)
{
    unsigned long gen_flush = vista_get_generation_flush_values();
    uint32_t gen = (uint32_t)(gen_flush >> 32UL);
    bool flush = (bool)(gen_flush & 0xFFFFFFFFUL);

    // Case 1. (g, nf) -> (g, nf) / (g, f) -> (g, f) : no action
    if (ctl->gen == gen && ctl->is_flush == flush)
        return;

    // All other cases: invalidate
    VistaSlruReset(ctl);
    ctl->gen = gen;
    ctl->is_flush = flush;    
}

void *VistaSlruReadPage(VistaSlruCtl ctl, int pageno)
{
    int i, slotno;

    VistaSlruInvalidateIfNeeded(ctl);

    slotno = VistaSlruFindSlot(ctl, pageno);

    /* Cache hit */
    if (slotno >= 0) {
        ctl->cur_lru_count++;
        ctl->lru_count[slotno] = ctl->cur_lru_count;
        return ctl->page_buffer[slotno].data;
    }

    /* Not found in cache, need to load */
    for (i = 0; i < ctl->num_slots; i++) {
        if (ctl->pageno[i] == VISTA_SLRU_SLOT_EMPTY) {
            slotno = i;
            break;
        }
    }

    ctl->cur_lru_count++;
    /* Need to evict the least recently used slot */
    if (slotno < 0) {
        int diff;
        int max_diff = -1;
        for (i = 0; i < ctl->num_slots; i++) {
            diff = ctl->cur_lru_count - ctl->lru_count[i];
            if (diff < 0) {
                ctl->lru_count[i] = ctl->cur_lru_count;
                diff = 0;
            }

            if (diff > max_diff) {
                max_diff = diff;
                slotno = i;
            }
        }
    }

    assert(slotno >= 0 && slotno < ctl->num_slots);

    ctl->pageno[slotno] = pageno;
    ctl->lru_count[slotno] = ctl->cur_lru_count;
    return VistaSlruLoadPage(ctl, slotno, pageno);
}

void VistaSlruReset(VistaSlruCtl ctl)
{
    for (int i = 0; i < ctl->num_slots; i++) {
        ctl->pageno[i] = VISTA_SLRU_SLOT_EMPTY;
        ctl->lru_count[i] = 0;
    }
    ctl->cur_lru_count = 0;
}
