#include "libvista.h"
#include "vista_chtable.h"

#include <errno.h>
#include <stdlib.h>

static __thread struct vista_chtable chtable;

struct vista_channel *vista_chtable_get(int idx) {
    if (unlikely(idx < 0 || idx >= VISTA_CHANNEL_TABLE_MAX))
        return NULL;

    if (unlikely(!chtable.used[idx]))
        return NULL;

    return &chtable.data[idx];
}

int vista_chtable_add(void) {
    for (int i = 0; i < VISTA_CHANNEL_TABLE_MAX; i++) {
        if (!chtable.used[i]) {
            chtable.data[i].channel_idx = i;
            chtable.used[i] = true;
            return i;
        }
    }
    return -ENOMEM;
}

int vista_chtable_del(int idx) {
    if (idx < 0 || idx >= VISTA_CHANNEL_TABLE_MAX)
        return -EINVAL;

    chtable.used[idx] = false;
    return 0;
}