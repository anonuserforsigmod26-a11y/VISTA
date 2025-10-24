#ifndef __VISTA_CHTABLE_H__
#define __VISTA_CHTABLE_H__

#include "libvista.h"

#define VISTA_CHANNEL_TABLE_MAX 128

struct vista_chtable {
    struct vista_channel data[VISTA_CHANNEL_TABLE_MAX];
    bool used[VISTA_CHANNEL_TABLE_MAX];
};

struct vista_channel *vista_chtable_get(int idx);
int vista_chtable_add(void);
int vista_chtable_del(int idx);

#endif
