#ifndef __VISTA_PG_SNAPSHOT_H__
#define __VISTA_PG_SNAPSHOT_H__
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VISTA_PG_SNAPSHOT_NAME_MAX 1024

int vista_get_snapshot_name(void *shrd_metadata_ptr, char *name, uint32_t* generation);
void vista_set_oldest_olap_generation(void *shrd_metadata_ptr, uint32_t olap_current_generation);

#ifdef __cplusplus
}
#endif

#endif /* __VISTA_PG_SNAPSHOT_H__ */
