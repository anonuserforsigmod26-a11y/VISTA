#ifdef CONFIG_VISTA
#include <linux/vista.h>
#include <linux/hugetlb.h>
#include <linux/rmap.h>
#include <linux/printk.h>
#include <linux/slab.h>
#include <linux/cpumask.h>
#include <linux/align.h>
#include <linux/smp.h>
#include <linux/page-flags.h>
#include <linux/pfn_t.h>
#include <linux/mman.h>
#include <linux/sched.h>
#include <linux/syscalls.h>

#include <uapi/linux/vista.h>

#include <asm/tlbflush.h>

bool vista_init = false;
struct vista_hugepage_mgr vista_hugepage_mgr;
static struct vista_cores vista_cores;
static struct list_head vista_task_list[NR_CPUS];

#ifdef CONFIG_VISTA_DEBUG
static void vista_dump_hugepage_mgr(void)
{
	int i;

	pr_info("[VISTA/INFO] OLAP: %d, OLTP: %d",
		vista_hugepage_mgr.nr_olap_pages,
		vista_hugepage_mgr.nr_oltp_pages);

	for (i = 0; i < vista_hugepage_mgr.nr_olap_pages; ++i) {
		pr_info("[VISTA/INFO] OLAP[%d] hugepage: %lu, 0x%lx", i,
			(unsigned long)folio_pfn(vista_hugepage_mgr.olap_folios[i]),
			(unsigned long)vista_hugepage_mgr.olap_folios[i]);
	}

	for (i = 0; i < vista_hugepage_mgr.nr_oltp_pages; ++i) {
		pr_info("[VISTA/INFO] OLTP[%d] hugepage: %lu, 0x%lx", i,
			(unsigned long)folio_pfn(vista_hugepage_mgr.oltp_folios[i]),
			(unsigned long)vista_hugepage_mgr.oltp_folios[i]);
	}
}

static void vista_dump_cpumask(void)
{
	pr_info("[VISTA/INFO] OLAP cpumask: %*pbl\n", NR_CPUS, &vista_cores.olap_mask);	
	pr_info("[VISTA/INFO] OLTP cpumask: %*pbl\n", NR_CPUS, &vista_cores.oltp_mask);
}

static void vista_dump_task_list(void)
{
	struct vista_task_node *pos;
	int cpu;

	pr_info("[VISTA/INFO] ==================== Task dump started. ====================");
	for (cpu = 0; cpu < NR_CPUS; ++cpu) {
		if (list_empty(&vista_task_list[cpu]))
			continue;

		list_for_each_entry(pos, &vista_task_list[cpu], list) {
			pr_info("[VISTA/INFO] --------- PID %d ---------", (int)pos->task->pid);
			pr_info("[VISTA/INFO] task pid, tgid: %d, %d",
				(int)pos->task->pid, (int)pos->task->tgid);
			pr_info("[VISTA/INFO] is_olap: %d", (int)pos->is_olap);
			pr_info("[VISTA/INFO] remap_window (user): 0x%lx", pos->u_remap_window);
			pr_info("[VISTA/INFO] remap_alert (kernel): 0x%lx", (unsigned long)pos->k_remap_alert);
		}
	}
	pr_info("[VISTA/INFO] ==================== Task dump ended. ====================");
}
#endif

static void __init vista_resv_mem_olap(struct hstate *h)
{
	int i;
	gfp_t mask = htlb_alloc_mask(h);
	unsigned int nr_olap_pages = h->free_huge_pages_node[VISTA_NID_OLAP];
	vista_hugepage_mgr.olap_folios =
		(struct folio **)kmalloc(sizeof(struct folio *) * nr_olap_pages,
				         GFP_KERNEL|__GFP_ZERO);

	for (i = 0; i < nr_olap_pages; ++i) {
		vista_hugepage_mgr.olap_folios[i] = 
			alloc_hugetlb_folio_reserve(h, VISTA_NID_OLAP, NULL, mask);

		if (!vista_hugepage_mgr.olap_folios[i]) {
			pr_warn("[VISTA/ERROR] hugepage reserve failed for OLAP\n");
			return;
		}

		folio_zero_segment(vista_hugepage_mgr.olap_folios[i], 0,
				   VISTA_HUGEPAGE_SIZE);
	}

	vista_hugepage_mgr.nr_olap_pages = nr_olap_pages;
}

static void __init vista_resv_mem_oltp(struct hstate *h)
{
	int i;
	gfp_t mask = htlb_alloc_mask(h);
	unsigned int nr_oltp_pages = h->free_huge_pages_node[VISTA_NID_OLTP];
	vista_hugepage_mgr.oltp_folios =
		(struct folio **)kmalloc(sizeof(struct folio *) * nr_oltp_pages,
					 GFP_KERNEL|__GFP_ZERO);
	
	for (i = 0; i < nr_oltp_pages; ++i) {
		vista_hugepage_mgr.oltp_folios[i] = 
			alloc_hugetlb_folio_reserve(h, VISTA_NID_OLTP, NULL, mask);

		if (!vista_hugepage_mgr.oltp_folios[i]) {
			pr_warn("[VISTA/ERROR] hugepage reserve failed for OLTP\n");
			return;
		}

		folio_zero_segment(vista_hugepage_mgr.oltp_folios[i], 0,
				   VISTA_HUGEPAGE_SIZE);
	}
	
	vista_hugepage_mgr.nr_oltp_pages = nr_oltp_pages;
}

/*
 * @brief: Reserve 1G hugepages for OLAP/OLTP.
 * @details: When the kernel is booted with GRUB config 
 * 'hugepagesz=1G hugepages=N', VISTA intercepts them at the boot-time.
 *
 * TODO: We may need a configuration for the count of the 1G hugepages for VISTA.
 * NOTE: OLAP pages are located at nid=0, OLTP pages are located at nid=1
 */
void __init vista_resv_hugepages(void)
{
	struct hstate *h;

	pr_info("[VISTA/INFO] hugepage reserve started.\n");

	for_each_hstate(h) {
		/*
		 * Kernel manages hugepages with hstate.
		 * We only need the 1G hugepage hstate.
		 */
		if (!hstate_is_gigantic(h))
			continue;

		BUG_ON(h->order != 18);

		vista_resv_mem_olap(h);
		vista_resv_mem_oltp(h);

		break;
	}

#ifdef CONFIG_VISTA_DEBUG
	vista_dump_hugepage_mgr();	
#endif

	pr_info("[VISTA/INFO] hugepage reserve ended.\n");
}

void __init vista_init_metadata(void)
{
	int i;

	cpumask_clear(&vista_cores.olap_mask);
	cpumask_clear(&vista_cores.oltp_mask);

	for (i = 0; i < NR_CPUS; ++i)
		INIT_LIST_HEAD(&vista_task_list[i]);

	WRITE_ONCE(vista_init, true);
}

static int vista_setup_backing_page(unsigned long addr, struct folio *folio,
				    pgoff_t idx)
{
	unsigned long pfn;
	struct hstate *h;
	struct vm_area_struct *vma;
	struct mm_struct *mm = current->mm;
	pgd_t *pgd;
	p4d_t *p4d;
	pud_t *pud;

	mmap_write_lock(mm);
	pgd = pgd_offset(mm, addr);
	p4d = p4d_alloc(mm, pgd, addr);
	pud = pud_alloc(mm, p4d, addr);

	vma = find_vma(mm, addr);	
	
	pfn = folio_pfn(folio);
	vista_insert_pfn_pud(vma, addr, pud, pfn_to_pfn_t(pfn), true);

	h = hstate_vma(vma);

	folio_ref_inc(folio);
	mmap_write_unlock(mm);

	/*
	 * Update the f_mapping information here. (for the child of the fork())
	 * (to provide mapping information when the page has been faulted)
	 */
	if (!hugetlbfs_pagecache_present(h, vma, addr)) {
		hugetlb_add_to_page_cache(folio, vma->vm_file->f_mapping, idx);
	}
	folio_unlock(folio); /* lock was acquired above */

	return 0;
}

/*
 * This function maps 1G hugepages to [addr, addr+count*1G] area.
 * Find corresponding pud entry and mark it as a leaf (=set PSE bit).
 * Map reserved hugepages to those entries.
 */
static int vista_setup_backing_pages(unsigned long addr, unsigned int count,
				     int flag)
{
	unsigned int i;
	unsigned long cur_addr = addr;
	struct folio *folio;

	for (i = 0; i < count; ++i, cur_addr += VISTA_HUGEPAGE_SIZE) {
		folio = (flag == VISTA_MAP_OLAP) ?
			vista_hugepage_mgr.olap_folios[i] :
			vista_hugepage_mgr.oltp_folios[i];

		vista_setup_backing_page(cur_addr, folio, (pgoff_t)i);	
	}

	return 0;
}

static int vista_install_null_entry(unsigned long addr)
{
	struct mm_struct *mm = current->mm;
	pgd_t *pgd;
	p4d_t *p4d;
	pud_t *pud;

	mmap_write_lock(mm);
	pgd = pgd_offset(mm, addr);
	p4d = p4d_alloc(mm, pgd, addr);
	pud = pud_alloc(mm, p4d, addr);

	pud_clear(pud);
	mmap_write_unlock(mm);

	return 0;
}

/*
 * Different point from original 'mmap' system call is, this function also does
 * page table mapping to instantly see the pages without page fault.
 */
static unsigned long vista_do_mmap(unsigned int count, int flag,
				   bool install_null)
{
	int mmap_flag = (MAP_SHARED | MAP_ANONYMOUS | MAP_HUGETLB |
			 MAP_HUGE_1GB | MAP_NORESERVE);
	unsigned long addr;
	unsigned long prot = PROT_READ | PROT_WRITE;
	unsigned int nr_pages = 
		(flag & VISTA_MAP_OLAP) ? vista_hugepage_mgr.nr_olap_pages :
					  vista_hugepage_mgr.nr_oltp_pages;
	
	/*
	 * For the remap window(OLAP -> OLTP) address, it should be R/O.
	 */
	if (install_null) {
		prot = PROT_READ;	
		nr_pages = 1;
	}

	if (count == 0 || count > nr_pages)
		return -EINVAL;

	BUG_ON(install_null && count != 1);

	/*
	 * Create an user address space.
	 */
	addr = ksys_mmap_pgoff(0, count * VISTA_HUGEPAGE_SIZE,
			       prot, mmap_flag, UINT_MAX, 0);	
	if (!addr)
		return -ENOMEM;

	BUG_ON(!IS_ALIGNED(addr, VISTA_HUGEPAGE_SIZE));

	/*
	 * For OLAP -> OLTP visibility, we need a 1GiB empty-mapped space.
	 * If the `install_null` is true, we just install null on the `pud`.
	 */
	if (install_null) {
		if (vista_install_null_entry(addr))
			return -ENOMEM;
		return addr;
	}

	/*
	 * Setup `count` backing pages to the user address space.
	 */
	if (vista_setup_backing_pages(addr, count, flag))
		return -ENOMEM;

	return addr;
}

/*
 * The ipi(vista_remap) handler.
 * We do following operations in here. 
 *
 * 1. Find the OLAP process.
 * 2. Access tsk->mm, find the pud and remap it.
 * 3. Increase the ref count.
 * 4. Mark it visible on user-kernel shared metadata region.
 */
static void vista_remap_handler(void *arg)
{
	int cpu = smp_processor_id();
	unsigned long pfn;
	pgd_t *pgd;
	p4d_t *p4d;
	pud_t *pud;
	unsigned int idx = *((unsigned int *)arg);
	struct vista_task_node *pos;

	list_for_each_entry(pos, &vista_task_list[cpu], list) {
		void *remap_alert = pos->k_remap_alert;
		unsigned long addr = pos->u_remap_window;
		struct mm_struct *mm;
		struct vm_area_struct *vma;

		if (unlikely(!pos->is_olap)) {
			WARN_ONCE(1, "[VISTA/WARN] OLTP task has got an IPI.");
			continue;
		}

		if (unlikely(!remap_alert || !addr)) {
			pr_warn("[VISTA/FATAL] OLAP task node has invalid info.");
			BUG();
		}

		if (unlikely(!pos->task->mm)) {
			pr_warn("[VISTA/FATAL] task->mm is NULL.");
			pr_warn("[VISTA/FATAL] ------ task informations ------");
			pr_warn("[VISTA/FATAL] pid: %d, tgid: %d, comm: %s",
				pos->task->pid, pos->task->tgid, pos->task->comm);
			BUG();
		}
		
#ifdef CONFIG_VISTA_DEBUG
		pr_info("[VISTA/INFO] remap handler (is_olap, addr, remap_alert, mm): %d, 0x%lx, 0x%lx, 0x%lx",
			(int)pos->is_olap, addr, (unsigned long)remap_alert,
			(unsigned long)pos->task->mm);
#endif

		mm = pos->task->mm;
		/*
		 * NOTE: Calling 'find_vma()' is not allowed in the interrupt
		 * context. It requires a mmap_lock before calling. Since we
		 * don't need the lock to be held, we exported find_vma()
		 * without mmap_lock assertion. (as vista_find_vma()).
		 */
		vma = vista_find_vma(mm, addr);
		
		/*
	 	* We don't need to get a write lock here. (what? really?)
	 	* (No one can access to this page table at the same time.)
	 	*/
		pgd = pgd_offset(mm, addr);
		p4d = p4d_offset(pgd, addr);
		pud = pud_offset(p4d, addr);

		pud_clear(pud);

		/*
		 * Read-only mapping for this hugepage.
	 	 */
		pfn = folio_pfn(vista_hugepage_mgr.oltp_folios[idx]);
		vista_insert_pfn_pud(vma, addr, pud, pfn_to_pfn_t(pfn), 0);
		folio_ref_inc(vista_hugepage_mgr.oltp_folios[idx]);

		/*
		 * XXX: THERE IS NO NEED TO FLUSH TLB.
		 *
		 * The remapping process only can be progressed after calling
		 * the 'vista_unmap()' or this is the first time to remap.
		 *
		 * 1. The VISTA user access before the remapping after the
		 * unmapping will get a #PF. (Because, VISTA OLAP runtime must
		 * have called the 'vista_unmap()' syscall before this illegal
		 * accessing. 'vista_unmap()' makes TLB shootdown when its
		 * unmapping the pud has done. = no TLB entry.)
		 *
		 * 2. When the user is accessing for the first time, there will
		 * be no valid TLB entry. (Plus, no mapped pud entry.)
		 */

		/*
		 * Now, this OLAP process can access OLTP data through remap window.
		 */
		atomic_set((atomic_t *)remap_alert, 1);
	}
}

/*
 * Make a shared region between user and kernel.
 */
static unsigned long vista_mmap_user_kernel_shrd_region(void **k_ptr)
{
	struct mm_struct *mm = current->mm;
	struct vm_area_struct *vma;
	int mmap_flag = (MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE);
	unsigned long addr;
	unsigned long prot = (PROT_READ | PROT_WRITE);

	/*
	 * Create user address space.
	 */
	addr = ksys_mmap_pgoff(0, PAGE_SIZE, prot, mmap_flag, UINT_MAX, 0);	
	if (!addr)
		return -ENOMEM;

	*k_ptr = (void *)page_to_virt(alloc_page(GFP_KERNEL | __GFP_ZERO)); 

	mmap_write_lock(mm);

	vma = find_vma(mm, addr);
	vm_insert_page(vma, addr, virt_to_page(*k_ptr));

	mmap_write_unlock(mm);

	return addr;
}

/*
 * Register the task to the vista_task_list.
 * Make sure preemption, IRQ disabled before entering this function.
 */
static void vista_register_task(struct task_struct *task, int cpu,
				void *k_remap_alert, int flag,
				unsigned long u_remap_window)
{
	struct cpumask *cpumask =
		(flag == VISTA_MAP_OLAP) ?
			&vista_cores.olap_mask : &vista_cores.oltp_mask;
							
	struct vista_task_node *node = (struct vista_task_node *)kmalloc(
		sizeof(struct vista_task_node), GFP_ATOMIC);

	node->task = task;
	node->k_remap_alert = k_remap_alert;
	node->is_olap = (flag == VISTA_MAP_OLAP);
	node->u_remap_window = u_remap_window;

	list_add_tail(&node->list, &vista_task_list[cpu]);

	/*
	 * Mark the cpu on the cpumask.
	 */
	cpumask_set_cpu(cpu, cpumask);	
}

/*
 * Unregister the task from list.
 * Make sure preemption, IRQ disabled before entering this function.
 */
struct vista_task_node *vista_unregister_task(struct task_struct *task)
{
	int cpu;
	bool is_olap, found = false;
	struct vista_task_node *pos, *tmp;

	/*
	 * At the kernel init time, do_exit() can be called.
	 * Then vista_unregister_task() is also called, causing to access
	 * uninitialized list (vista_task_list).
	 *
	 * Below code resolve this issue.
	 */
	if (unlikely(!READ_ONCE(vista_init)))
		return NULL;

	for (cpu = 0; cpu < NR_CPUS; ++cpu) {
		list_for_each_entry_safe(pos, tmp, &vista_task_list[cpu], list) {
			if (pos->task == task) {
				list_del(&pos->list);
				found = true;
				is_olap = pos->is_olap;
				break;
			}
		}
		
		if (found)
			break;
	}

	if (!found)
		return NULL;

	for (cpu = 0; cpu < NR_CPUS; ++cpu) {
		if (list_empty(&vista_task_list[cpu])) {
			struct cpumask *cpumask = is_olap ?
				&vista_cores.olap_mask : &vista_cores.oltp_mask;	
			cpumask_clear_cpu(cpu, cpumask);
		}
	}

	return pos;
}

/*********************** SYSTEM CALL IMPLEMENTATION ***************************/
/*
 * @brief: 'vista_u_mmap' system call implementation.
 * @details: Map reserved hugepages to user address space distinguished by flag.
 * @count: The count of the required hugepages.
 * @flag: VISTA_MAP_OLAP, VISTA_MAP_OLTP
 * @user_addrs: The mapped user addresses. (return)
 */
SYSCALL_DEFINE3(vista_mmap, unsigned int, count, int, flag,
			      struct vista_addrs __user *, user_addrs)
{
	struct vista_addrs vista_user_addrs;
	/*
	 * u_* represents user address.
	 * k_* represents kerenel address.
	 */
	unsigned long u_olap = 0, u_remap_window = 0;
	unsigned long u_oltp = 0, u_remap_alert = 0;
	void *k_remap_alert = NULL;
	int cpu;

	switch (flag) {
	case VISTA_MAP_OLTP:
		u_oltp = vista_do_mmap(count, flag, false);
		break;

	case VISTA_MAP_OLAP:
		u_olap = vista_do_mmap(count, flag, false);
		u_remap_window = vista_do_mmap(1, 0, true);
		u_remap_alert = vista_mmap_user_kernel_shrd_region(&k_remap_alert);
		break;

	default:
		pr_warn("[VISTA/ERROR] 'vista_u_mmap' flag is invalid.\n");
		return -EINVAL;
	}

	local_irq_disable();
	cpu = get_cpu();

	vista_register_task(current, cpu, k_remap_alert, flag, u_remap_window);

	put_cpu();
	local_irq_enable();

	/*
	 * NOTE:
	 * For OLAP workers, every fields in `user_addrs` are valid.
	 * For OLTP workers, only `oltp_ptr` is valid.
	 */
	vista_user_addrs.olap_ptr = u_olap;
	vista_user_addrs.oltp_ptr = u_oltp;
	vista_user_addrs.remap_window_ptr = u_remap_window;
	vista_user_addrs.remap_alert_ptr = u_remap_alert;

#ifdef CONFIG_VISTA_DEBUG
	pr_info("[VISTA/INFO] vista_u_mmap(%u, %d): 0x%lx, 0x%lx, 0x%lx, 0x%lx",
		count, flag, u_olap, u_oltp, u_remap_window, u_remap_alert);
	vista_dump_cpumask();
	vista_dump_task_list();
#endif

	if (copy_to_user(user_addrs, &vista_user_addrs, sizeof(struct vista_addrs)))
		return -EFAULT;

	return 0;
}

/*
 * @brief: 'vista_munmap()' syscall implementaion.
 * @detail: munmap allocated virtual address spaces which was allocated by
 * 'vista_mmap()' syscall.
 */
SYSCALL_DEFINE2(vista_munmap, struct vista_addrs __user *, user_addrs,
				unsigned int, count)
{
	int ret;
	struct vista_task_node *vista_task_node;
	struct vista_addrs vista_user_addrs;
	if (copy_from_user(&vista_user_addrs, user_addrs,
			   sizeof(struct vista_addrs)))
		return -EFAULT;

	local_irq_disable();
	preempt_disable();

	vista_task_node = vista_unregister_task(current);

	preempt_enable();
	local_irq_enable();

	/*
	 * `kfree()` cannot be called in above condition(preemption, IRQ disabled)
	 * therefore, we free it at here.
	 */
	if (likely(vista_task_node)) {
		free_page((unsigned long)vista_task_node->k_remap_alert);
		kfree(vista_task_node);
	}

	if (vista_user_addrs.olap_ptr) {
		/* Case for the OLAP */
		unsigned long u_olap = vista_user_addrs.olap_ptr;
		unsigned long u_remap_window = vista_user_addrs.remap_window_ptr;
		unsigned long u_remap_alert = vista_user_addrs.remap_alert_ptr;

		if ((ret = vm_munmap(untagged_addr(u_olap),
				     count * VISTA_HUGEPAGE_SIZE))) 
			return ret;

		if ((ret = vm_munmap(untagged_addr(u_remap_window),
				     VISTA_HUGEPAGE_SIZE)))
			return ret;

		if ((ret = vm_munmap(untagged_addr(u_remap_alert),
				     VISTA_METADATA_SIZE)))
			return ret;

	} else if (vista_user_addrs.oltp_ptr) {
		/* Case for the OLTP */
		unsigned long u_oltp = vista_user_addrs.oltp_ptr;

		if ((ret = vm_munmap(untagged_addr(u_oltp),
				     count * VISTA_HUGEPAGE_SIZE))) 
			return ret;

	} else {
		return -EINVAL;
	}

#ifdef CONFIG_VISTA_DEBUG
	vista_dump_cpumask();
	vista_dump_task_list();
#endif
	
	return ret;
}

/*
 * @brief: 'vista_remap' system call implementation
 * @details: Map the `idx` hugepage to OLAP's virtual address space.
 * When an OLTP worker triggered sealing, `libvista` should call this function.
 */
SYSCALL_DEFINE1(vista_remap, unsigned int, idx)
{
	const cpumask_t *mask = &vista_cores.olap_mask;

	if (idx < 0 || idx >= vista_hugepage_mgr.nr_oltp_pages)
		return -EINVAL;

	smp_call_function_many(mask, vista_remap_handler, (void *)&idx, true);

	return 0;
}

/*
 * @brief: 'vista_unmap' system call implementation
 * @details: Unmap hugepages from the page table, **IMPORTANT** this function
 * does not release the vma resource, only remaps pud entry to NULL.
 */
SYSCALL_DEFINE1(vista_unmap, unsigned long, addr)
{
	struct folio *folio;
	unsigned long pfn;
	pgd_t *pgd;
	p4d_t *p4d;
	pud_t *pud;
	struct vm_area_struct *vma;
	struct mm_struct *mm = current->mm;

	addr = untagged_addr(addr);
	if (!IS_ALIGNED(addr, VISTA_HUGEPAGE_SIZE))
		return -EINVAL;
		
	mmap_write_lock(mm);

	vma = find_vma(mm, addr);
	if (vma->vm_end - vma->vm_start != VISTA_HUGEPAGE_SIZE)
		return -EINVAL;

	pgd = pgd_offset(mm, addr);
	p4d = p4d_offset(pgd, addr);
	pud = pud_offset(p4d, addr);

	pfn = pud_pfn(*pud);

	/*
	 * One single pud entry unmapping.
	 */
	pud_clear(pud);

	mmap_write_unlock(mm);

	/*
	 * TLB shootdown for this mapping.
	 */
	flush_tlb_range(vma, addr, addr + VISTA_HUGEPAGE_SIZE);

	/*
	 * Find the corresponding hugepage and decrease ref count. 
	 */
	folio = pfn_folio(pfn);
	folio_ref_dec(folio);

	return 0;
}

#endif /* CONFIG_VISTA */
