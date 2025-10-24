#ifdef USE_VISTA_IO

#include <stdio.h>
#include <numa.h>
#include "vista_wrapper.h"
// #include "vista_remapped_buffer_access.h"

/* global vista state is managed inside vista_remapped_buffer_access.c */

static void* g_vista_addrs = NULL;

// Constructor - runs when libscan.so is loaded
__attribute__((constructor))
static void vista_library_init(void) {
	// Set NUMA memory affinity to node 0
	if (numa_available() >= 0) {
		numa_set_preferred(0);
	}

#ifdef USE_VISTA_REMAP
	vista_wrapper_scan_init();
	//fprintf(stderr, "[libscan] Vista scan initialized + OLAP registered\n");
#else
        g_vista_addrs = vista_wrapper_alloc_addrs();
	vista_wrapper_register_olap(g_vista_addrs);
#endif

//        g_vista_addrs = vista_wrapper_alloc_addrs();
//        if (g_vista_addrs) {
//            if (vista_wrapper_register_olap(g_vista_addrs) == 0) {
//                g_vista_registered = 1;
//                fprintf(stderr, "[libscan] Vista OLAP registered at library load\n");
//                
//#ifdef USE_VISTA_REMAP
//                // Initialize Vista scan
//                vista_wrapper_scan_init();
//                fprintf(stderr, "[libscan] Vista scan initialized\n");
//#endif
//            } else {
//                vista_wrapper_free_addrs(g_vista_addrs);
//                g_vista_addrs = NULL;
//                fprintf(stderr, "[libscan] Failed to register Vista OLAP\n");
//            }
//        }
//    }
}

// Destructor - runs when libscan.so is unloaded
__attribute__((destructor))
static void vista_library_cleanup(void) {
#ifdef USE_VISTA_REMAP
        // Close Vista scan
        vista_wrapper_scan_close();
        //fprintf(stderr, "[libscan] Vista scan closed\n");
#else
	vista_wrapper_unregister_olap(g_vista_addrs);
	vista_wrapper_free_addrs(g_vista_addrs);
	g_vista_addrs= NULL;
#endif	

//	if (g_vista_registered && g_vista_addrs) {
//#ifdef USE_VISTA_REMAP
//        // Close Vista scan
//        vista_wrapper_scan_close();
//        fprintf(stderr, "[libscan] Vista scan closed\n");
//#endif
//        
//        vista_wrapper_unregister_olap(g_vista_addrs);
//        vista_wrapper_free_addrs(g_vista_addrs);
//        g_vista_addrs = NULL;
//        g_vista_registered = 0;
//        fprintf(stderr, "[libscan] Vista OLAP unregistered at library unload\n");
//    }
}

#endif /* USE_VISTA_IO */
