#include "vista_cache_loader.hpp"
#include <algorithm>
#include <set>
#include <cassert>
#include <iostream>

void* VistaCacheLoader::getPage(uint64_t page_num) {
    if (page_active && current_page_num == page_num) {
        return current_page;  // Already have this page
    }

    // Release current page before getting new one
    releasePage();

    // Get new page from Vista
    current_page = vista_wrapper_get_page(vista_desc, VISTA_NO_REMAP_ACCESS_VALUE);

    if (current_page == (void*)VISTA_NO_MORE_PAGES_TO_READ_VALUE ||
        current_page == (void*)VISTA_INVALID_VD_VALUE ||
        current_page == (void*)VISTA_NO_PUT_PAGE_CALLED_VALUE) {
        current_page = nullptr;
        return nullptr;
    }

    current_page_num = page_num;
    page_active = true;
    return current_page;
}

void VistaCacheLoader::releasePage() {
    if (page_active && current_page) {
        vista_wrapper_put_page(vista_desc, current_page);
        current_page = nullptr;
        page_active = false;
    }
}

bool VistaCacheLoader::initializeVista(int cache_fd, const std::vector<VectorInfo>& vectors) {
    // Calculate all pages needed for these vectors
    std::set<uint64_t> needed_pages;

    for (const auto& vector : vectors) {
        uint64_t start_page = vector.file_offset / VISTA_PAGE_SIZE;
        uint64_t end_page = (vector.file_offset + vector.size - 1) / VISTA_PAGE_SIZE;

        for (uint64_t page = start_page; page <= end_page; page++) {
            needed_pages.insert(page);
        }
    }

    if (needed_pages.empty()) {
        return false;
    }

    // Convert pages to contiguous ranges for Vista
    std::vector<vista_read_range> ranges;
    auto it = needed_pages.begin();
    uint64_t range_start = *it;
    uint64_t range_end = *it;

    for (++it; it != needed_pages.end(); ++it) {
        if (*it == range_end + 1) {
            // Contiguous page - extend current range
            range_end = *it;
        } else {
            // Gap found - finalize current range and start new one
            vista_read_range range;
            range.fd = cache_fd;
            range.start = range_start;
            range.end = range_end + 1;  // Vista uses exclusive end
            ranges.push_back(range);

            range_start = range_end = *it;
        }
    }

    // Add final range
    vista_read_range range;
    range.fd = cache_fd;
    range.start = range_start;
    range.end = range_end + 1;  // Vista uses exclusive end
    ranges.push_back(range);

    // Initialize Vista using wrapper
    vista_desc = vista_wrapper_open_custom(ranges.data(), ranges.size(), VISTA_PAGE_SIZE, 0);
    return vista_desc >= 0;
}

void VistaCacheLoader::readVectorData(std::vector<uint8_t>& dest, uint64_t offset, uint32_t size) {
    dest.resize(size);

    uint64_t start_page = offset / VISTA_PAGE_SIZE;
    uint64_t end_page = (offset + size - 1) / VISTA_PAGE_SIZE;
    uint64_t bytes_copied = 0;

    // Iterate through all pages spanned by this vector
    for (uint64_t page_num = start_page; page_num <= end_page; page_num++) {
        void* page = getPage(page_num);
        if (!page) {
            throw std::runtime_error("Failed to get page from Vista");
        }

        uint8_t* page_data = static_cast<uint8_t*>(page);

        // Calculate the range to copy within this page
        uint64_t page_offset = 0;
        uint32_t copy_size = VISTA_PAGE_SIZE;

        // First page: start from the offset within the page
        if (page_num == start_page) {
            page_offset = offset % VISTA_PAGE_SIZE;
            copy_size = VISTA_PAGE_SIZE - page_offset;
        }

        // Last page: only copy remaining bytes
        if (page_num == end_page) {
            uint32_t remaining = size - bytes_copied;
            copy_size = std::min(copy_size, remaining);
        }

        // Copy data from this page
        std::copy(page_data + page_offset, page_data + page_offset + copy_size, dest.data() + bytes_copied);
        bytes_copied += copy_size;
    }
}

void VistaCacheLoader::cleanup() {
    releasePage();

    if (vista_desc >= 0) {
        vista_wrapper_close(vista_desc);
        vista_desc = -1;
    }

    // Reset to initial state for reuse
    current_page = nullptr;
    current_page_num = UINT64_MAX;
    page_active = false;
}
