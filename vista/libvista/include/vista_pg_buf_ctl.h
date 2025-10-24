#ifndef __VISTA_PG_BUF_CTL_H__
#define __VISTA_PG_BUF_CTL_H__

#include "libvista.h"
#include "vista_seqlock.h"
#include "vista_pg_read_buffer.h"

struct vista_uring_user_data vista_get_gen_flush_info(void *shrd_metadata_ptr);

#endif