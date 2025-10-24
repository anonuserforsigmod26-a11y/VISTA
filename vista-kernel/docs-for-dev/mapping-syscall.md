# Mapping system call for VISTA users.
## 560. `int vista_mmap(unsigned int count, int flag, struct vista_addrs *addr)`
VISTA owns whole 1GiB hugepages in the kernel, therefore, to use its space we need a syscalls.

This system call always maps hugepages in specific order.

`vista_mmap` maps `count` hugepages to user address space.

Also, by this system call, user's (core, task) pair is registered to VISTA.

(Therefore, the user should fix the process' affinity to the specific core before calling it.)

### args
* count: The count of the hugepages needed.
* flag: `VISTA_MAP_OLAP`, `VISTA_MAP_OLTP` is available.
    * `VISTA_MAP_OLAP=0x1`: Maps `count` hugepages(from nid=0).
    * `VISTA_MAP_OLTP=0x2`: Maps `count` hugepages(from nid=1).

### return
int, for the error status.

All return addresses are included in 
`struct vista_addrs *`, which represents the start addresses of those hugepages.
* `vista_addrs`
    * `olap_ptr`: `count` OLAP hugepage backed virtual address space.
    * `oltp_ptr`: `count` OLTP hugepage backed virtual address space.
    * `remap_window_ptr`: A virtual address space that used when the remapping has been occured.
    * `remap_alert_ptr`: User-Kernel shared memory region to alert whether it's remapped or not.
    * `shrd_metadata_ptr`: An empty pointer that can be used to store the adress of the OLTP-OLAP shared memory region. Caution: this is NOT managed by the kernel. It is user's responsibility to correctly allocate a shared memory segment and initialize this pointer. VISTA user library is expected to call `shmget` to do this job.  

NOTE:
* For OLAP, every fields except `oltp_ptr` are valid.
* For OLTP, only `oltp_ptr` is valid. (NULL for else)

### errors
* EINVAL: `flag` is invalid or `count` is out of bound.
* ENOMEM: failed to create new user address space(rare to happen).

## 561. `int vista_munmap(struct vista_addrs *addrs, unsigned int count)`
Munmap the addresses which were allocated by `vista_mmap()`.

Plus, it unregisters (core, task) from the VISTA system.

### args
* addr: The start addresses which was allocated by `vista_mmap()`
* count: The count of the hugepages which was mapped by `vista_mmap()`

### return
Same as `munmap()` system call.

## 562. `int vista_remap(unsigned int idx)`
OLTP hugepage is remapped to OLAP processes.

### args
* idx: The OLTP hugepage index to make it remapped.

### return
`int`, 0 when successed.

### errors
* EINVAL: `idx` is out of the bound.

## 563. `int vista_unmap(unsigned long addr)`
Drop the physical memory at `addr`. (Unmap the pud)

** CAUTION: THIS DOES NOT FREE THE USER ADDRESS SPACE **

### args
* addr: virtual address to unmap. **(the address for OLAP->OLTP visibility, `remap_window_ptr`)**

### return
`int`, 0 when successed.

### errors
* EINVAL: `addr` is invalid.
