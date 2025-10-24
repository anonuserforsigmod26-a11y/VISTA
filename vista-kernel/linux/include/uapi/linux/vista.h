#ifndef __VISTA_UAPI_H
#define __VISTA_UAPI_H

struct vista_addrs {
	unsigned long olap_ptr;
	unsigned long remap_window_ptr;
	unsigned long remap_alert_ptr;
	unsigned long oltp_ptr;
    unsigned long shrd_metadata_ptr; /* this is not managed by vista kernel library */
};

#endif /* __VISTA_UAPI_H */
