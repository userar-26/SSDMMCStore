#include "kv_store_internal.h"

int kvs_init(size_t storage_size_bytes)
{
    // Проверяем, не инициализировано ли уже хранилище
    if (device)
        return -1;

    device = malloc(sizeof(SSDMMCDevice));

    // Проверяем, удалось ли выделить память под структуру
    if (!device)
        return -2;

    // Заполняем параметры хранилища
    device->logical_size_bytes  = storage_size_bytes;
    device->page_count          = ctrl_get_page_count();
    device->word_size_bytes     = ctrl_get_word_size();
    device->words_per_page      = ctrl_get_words_per_page();
    device->page_size_bytes     = device->word_size_bytes * device->words_per_page;
    device->physical_size_bytes = device->page_count * device->page_size_bytes;

    // Проверяем, достаточно ли физического размера
    if (device->physical_size_bytes < device->logical_size_bytes) {
        free(device);
        device = NULL;
        return -3;
    }

    if (kvs_create_storage_file(KVS_STORAGE_FILENAME, device->physical_size_bytes) != 0) {
        kvs_log("Не удалось создать файл с данными");
        free(device);
        device = NULL;
        return -4;
    }

    if (kvs_create_metadata_file(KVS_METADATA_FILENAME) != 0) {
        kvs_log("Не удалось создать файл с метаданными");
        free(device);
        device = NULL;
        return -5;
    }

    if (kvs_bitmap_init(KVS_BITMAP_FILENAME, device->page_count, &device->bitmap, &device->bitmap_size_bytes) != 0) {
        kvs_log("Не удалось инициализировать битовую карту");
        free(device);
        device = NULL;
        return -6;
    }

    if (kvs_page_stats_init(KVS_PAGE_STATS_FILENAME, device->page_count, &device->page_rewrite_count, &device->page_rewrite_count_size) != 0) {
        kvs_log("Не удалось инициализировать массив статистики перезаписи");
        free(device->bitmap);
        free(device);
        device = NULL;
        return -7;
    }

    return 0;
}



void kvs_deinit(void)
{
    // Проверяем, инициализировано ли хранилище
    if (!device)
        return;

    if (kvs_clear_storage_file(KVS_STORAGE_FILENAME) != 0) {
        kvs_log("Не удалось очистить файл с данными");
    }

    if (kvs_clear_metadata_file(KVS_METADATA_FILENAME) != 0) {
        kvs_log("Не удалось очистить файл с метаданными");
    }

    if (kvs_clear_bitmap_file(KVS_BITMAP_FILENAME, device->bitmap_size_bytes) != 0) {
        kvs_log("Не удалось очистить файл с битовой картой");
    }

    if (kvs_clear_page_stats_file(KVS_PAGE_STATS_FILENAME, device->page_rewrite_count_size) != 0) {
        kvs_log("Не удалось очистить файл с массивом статистики перезаписей");
    }

    // Освобождаем динамические массивы, если они были выделены
    free(device->bitmap);
    free(device->page_rewrite_count);

    free(device);
    device = NULL;
}


