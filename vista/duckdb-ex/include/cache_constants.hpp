#pragma once

// Cache-related constants
#define DEFAULT_BLOCK_GROUP_SIZE (1<<15) // Number of blocks per BlockGroup
#define DEFAULT_SUB_GROUP_SIZE (1<<11)     // Number of blocks per SubGroup
#define BLOCK_SIZE 8192                // PostgreSQL block size (8KB)

// Derived constants
#define MAX_SUBGROUPS_PER_BG (DEFAULT_BLOCK_GROUP_SIZE / DEFAULT_SUB_GROUP_SIZE)
#define SUBGROUP_DATA_SIZE (DEFAULT_SUB_GROUP_SIZE * BLOCK_SIZE)  // 256 * 8KB = 2MB

static_assert(DEFAULT_BLOCK_GROUP_SIZE % DEFAULT_SUB_GROUP_SIZE == 0,
              "DEFAULT_BLOCK_GROUP_SIZE must be divisible by DEFAULT_SUB_GROUP_SIZE");
