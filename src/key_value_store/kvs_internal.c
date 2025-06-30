#include "kvs_internal.h"
#include <time.h>

kvs_device *device = NULL;

void kvs_log(const char *format, ...) {
    FILE *log_file;
    if ((log_file = fopen(KVS_LOG_FILENAME, "a")) == NULL) return;
    time_t now;
    time(&now);
    struct tm *local_time = localtime(&now);
    char time_buffer[80];
    strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S", local_time);
    fprintf(log_file, "[%s] ", time_buffer);
    va_list args;
    va_start(args, format);
    vfprintf(log_file, format, args);
    fprintf(log_file, "\n");
    va_end(args);
    fclose(log_file);
}

uint32_t align_up(uint32_t size, uint32_t align) {
    if (align == 0) return size;
    return ((size + align - 1) / align) * align;
}

void kvs_free_device() {
    if (!device) {
        return;
    }
    if (device->fp) {
        fclose(device->fp);
    }
    if (device->key_index) {
        free(device->key_index);
    }
    if (device->page_crc.entry_crc) {
        free(device->page_crc.entry_crc);
    }
    if (device->page_rewrite_count) {
        free(device->page_rewrite_count);
    }
    if (device->metadata_bitmap) {
        free(device->metadata_bitmap);
    }
    if (device->bitmap) {
        free(device->bitmap);
    }
    free(device);
    device = NULL;
}