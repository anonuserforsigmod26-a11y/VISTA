#ifndef __VISTA_LIBSCAN_SNAPSHOT_H__
#define __VISTA_LIBSCAN_SNAPSHOT_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
void *VistaGetShmem(void);
int VistaGetSnapshotName(char *name, uint32_t* generation);
void *VistaImportSnapshot(const char *idstr);
int  VistaFreeSnapshot(void *snapshot);

#ifdef __cplusplus
}
#endif

#endif /* __VISTA_LIBSCAN_SNAPSHOT_H__ */
