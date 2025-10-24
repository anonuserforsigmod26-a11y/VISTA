#ifndef MEMORY_UTILS_H
#define MEMORY_UTILS_H

#include <stdlib.h>

/* Simple memory management functions for PostgreSQL compatibility */
static inline void* simple_malloc(size_t size) {
    return malloc(size);
}

static inline void simple_free(void *ptr) {
    if (ptr) free(ptr);
}

#endif /* MEMORY_UTILS_H */