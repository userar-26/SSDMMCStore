#include "kv_store_internal.h"

SSDMMCDevice *device = nullptr;

void kvs_log(const char *format, ...) {
    FILE *log_file = fopen(KVS_LOG_FILENAME, "a"); // Открываем для добавления
    if (!log_file) return; // Если не удалось открыть, просто выходим

    va_list args;
    va_start(args, format);
    vfprintf(log_file, format, args);
    fprintf(log_file, "\n");
    va_end(args);

    fclose(log_file);
}

