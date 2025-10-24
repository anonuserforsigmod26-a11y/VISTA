* Please check shared_buffers, work_mem, max_worker_processes, max_parallel_workers, max_parallel_workers_per_gather!
* Effectively disabled checkpointer, and explicitly disabled autovacuum.
* For VISTA, we must turn off the bgwriter by setting its lru_maxpages to 0.
* Other settings are based on well-known tuning parameters.
