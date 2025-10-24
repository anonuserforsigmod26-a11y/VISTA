# Core-Task Pair Registering
## Why is this needed?
To send IPIs (`smp_call_function_many()`), we need a cpumask that represents which core is running OLAP processes/threads.

Therefore, when the process/thread calls `vista_mmap()`, the VISTA subsystem registers (core, task) pair into its private space.

At that time, we marks the cpu to the cpumask.

(We assumes process will NEVER be rescheduled to the other cores.)

### The IPI Handler
We implement the IPI handler as `vista_remap_handler()` in kernel.

This function does following things.

1. Get a cpu number that current is running.
2. We have the (core, task) pair, so we can access task's `mm`.
3. Remap the requested OLTP hugepage to the reserved address(`remap_window_ptr`).
4. Local TLB flush to guarantee consistency.
