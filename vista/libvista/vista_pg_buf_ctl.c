#include "libvista.h"
#include "vista_seqlock.h"
#include "vista_pg_read_buffer.h"
#include "vista_pg_buf_ctl.h"

struct vista_uring_user_data vista_get_gen_flush_info(void *shrd_metadata_ptr)
{
    uint32_t old;
    char *base = (char *)shrd_metadata_ptr;
    vista_bufferpool_offsets *offsets = (vista_bufferpool_offsets *)(base + *((int *)base));
    libvista_BufferPoolControl *buf_ctl = (libvista_BufferPoolControl *)(base + offsets->buf_ctl_offset);
    struct vista_uring_user_data ret;

    do {
        old = libvista_seqlock_read_begin(&buf_ctl->ctl_seqlock);
        ret.generation = buf_ctl->global_generation;
        ret.is_flushing = buf_ctl->vista_op_mode;
    } while (unlikely(libvista_seqlock_read_retry(&buf_ctl->ctl_seqlock, old)));

    return ret;
}