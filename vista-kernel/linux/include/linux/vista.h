/* SPDX-License-Identifier: GPL-2.0 */
#ifndef __LINUX_VISTA_H
#define __LINUX_VISTA_H
#ifdef CONFIG_VISTA
#include <linux/mm_types.h>
#include <linux/sched.h>
#include <linux/cpumask.h>

/* NUMA node id for each OLAP/OLTP */
#define VISTA_NID_OLAP (0)
#define VISTA_NID_OLTP (1)

/* 'vista_u_mmap' flags */
#define VISTA_MAP_OLAP      (0x1)
#define VISTA_MAP_OLTP      (0x2)

#define VISTA_HUGEPAGE_SIZE (1UL << 30)
#define VISTA_HUGEPAGE_SHIFT (30)

/* Currently, a single page (4KiB) */
#define VISTA_METADATA_SIZE (4096)

struct vista_hugepage_mgr {
	struct folio **olap_folios;
	struct folio **oltp_folios;
	unsigned int nr_olap_pages;
	unsigned int nr_oltp_pages;
};
extern struct vista_hugepage_mgr vista_hugepage_mgr;

struct vista_cores {
	cpumask_t olap_mask;
	cpumask_t oltp_mask;
};

struct vista_task_node {
	bool is_olap;
	struct task_struct *task;
	void *k_remap_alert;
	unsigned long u_remap_window; /* XXX: THIS IS AN USER ADDRESS */
	struct list_head list;
};

void __init vista_resv_hugepages(void);
void __init vista_init_metadata(void);

struct vista_task_node *vista_unregister_task(struct task_struct *task);

#endif /* CONFIG_VISTA */
#endif /* _LINUX_VISTA_H */
