# Hugepage Reservation at Boot-time
For VISTA, we reserve 1GiB hugepages at boot-time to take control. No other systems can 
access our reserved hugepages. If the VISTA user configures 'hugepages=N hugepagesz=1G', 
we steal those pages during kernel booting.

### Implementation
In Linux kernel(6.12), hugepages are managed by `hstate`. Each `hstate` has its order, 
which represents the size of the hugepage, for the 1GiB, order=18 (2^18 * 4KiB = 1GiB).
Therefore, after the hugepage initialization, we execute our init function that steals 
those pages from `hstate(order: 18)` with `alloc_hugetlb_folio_reserve()`.

After the reservation, VISTA owns whole hugepages (via `vista_hugepage_mgr`). We properly 
manage the reference count to never freed to original hugepage subsystem(`hugetlbfs`).
The following interfaces are provided to access our reserved hugepages.

* System Calls
    * `int vista_mmap()`
    * `int vista_munmap()`
    * `int vista_remap()`
    * `int vista_unmap()`
