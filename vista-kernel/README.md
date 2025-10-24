# vista-kernel
The customized Linux kernel for VISTA.

Based on the Linux v6.12.

### How to compile/install the kernel.

`make oldconfig`

`make menuconfig`

Make sure following configs on.

* `CONFIG_VISTA`
* `CONFIG_TRANSPARENT_HUGEPAGE` (madvise)

`make -j`

`make modules -j`

`make modules_install -j`

`make headers -j`

`make headers_install INSTALL_HDR_PATH=/usr`

### Requirements

The running system must have two NUMA nodes. (We assumed it in our codes.)

### Added/Modificated code directory
* linux/vista/vista.c
    * The main source code of the VISTA OS module.
* linux/include/uapi/linux/vista.h
    * The user interface defined to use VISTA OS module.
        * (*For now, this file just only provides definition of `struct vista_addrs`.*)
* linux/include/vista.h
    * The header file of the `vista.c`.

Belows are changes of the original kernel source codes.

* linux/mm/huge_mm.h
    * To export `insert_pfn_pud()`. (exported as `vista_insert_pfn_pud()`)
* linux/mm/huge_memory.c
    * To export `insert_pfn_pud()`. (exported as `vista_insert_pfn_pud()`)
* linux/init/main.c
    * To call init function for VISTA OS module. (`vista_init_metadata()`)
* linux/mm/hugetlb.c
    * To call hugepage reservation function for VISTA OS module. (`vista_resv_hugepages()`)
* linux/include/linux/mm.h
    * export `find_vma()` as `vista_find_vma()`
* linux/mm/mmap.c
    * Definition of the `vista_find_vma()`
* linux/kernel/exit.c
    * Handles when the user didn't call `vista_munmap()` or the process was abnormally exited.
* x86 system call table.
    * Add four syscalls.

