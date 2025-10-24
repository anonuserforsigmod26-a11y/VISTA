#pragma once

#include <vector>
#include <cstdint>
#include <stdexcept>

extern "C" {
#include "vista_wrapper.h"
}

// Vista page size
#define VISTA_PAGE_SIZE 4096UL

// Vista range structure (from libvista.h)
struct vista_read_range {
    int fd;
    uint64_t start;
    uint64_t end;
};

class VistaCacheLoader {
public:
    struct VectorInfo {
        uint64_t file_offset;
        uint32_t size;
    };

private:
    int vista_desc;
    void* current_page;
    uint64_t current_page_num;
    bool page_active;

    // Get page from Vista (sequential access pattern)
    void* getPage(uint64_t page_num);

    // Release current page if active
    void releasePage();

public:
    VistaCacheLoader() : vista_desc(-1), current_page(nullptr), current_page_num(UINT64_MAX), page_active(false) {}
    ~VistaCacheLoader() { cleanup(); }

    // Initialize Vista with ranges needed for all vectors in a SubGroup
    bool initializeVista(int cache_fd, const std::vector<VectorInfo>& vectors);

    // Read vector data (supports arbitrary size spanning multiple pages)
    void readVectorData(std::vector<uint8_t>& dest, uint64_t offset, uint32_t size);

    // Clean up Vista resources
    void cleanup();
};
