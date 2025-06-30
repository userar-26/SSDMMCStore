#include "kvs_valid.h"
#include "kvs_internal_io.h"
#include "kvs_metadata.h"

int is_bitmap_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    uint32_t calculated_crc = crc32_calc(device->bitmap, device->superblock.bitmap_size_bytes);
    if (calculated_crc != device->page_crc.bitmap_crc) {
        return 0;
    }

    return 1;
}

int is_page_rewrite_count_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    uint32_t data_size = device->superblock.page_crc_offset - device->superblock.page_rewrite_offset;
    uint32_t temp_crc = crc32_calc(device->page_rewrite_count, data_size);
    if (temp_crc != device->page_crc.rewrite_crc) {
        return 0;
    }
    return 1;
}

int is_metadata_entry_valid(const kvs_metadata *metadata)
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (!metadata) {
        return KVS_INTERNAL_ERR_NULL_PARAM;
    }
    // Проверяем, что размеры и смещения находятся в допустимых границах
    if (metadata->value_size > device->superblock.userdata_size_bytes || metadata->value_offset >= device->superblock.metadata_offset   || metadata->value_offset < device->superblock.data_offset) {
        return 0;
    }
    // Проверяем, не является ли область данных просто стертой (состоит из 0xFF)
    uint32_t aligned_size = align_up(metadata->value_size, device->superblock.word_size_bytes);
    int empty_check = is_data_region_empty(metadata->value_offset, aligned_size);
    if (empty_check < 0) {
        return empty_check;
    }
    if (empty_check == 1) {
        return 0;
    }
    return 1;
}

int is_metadata_bitmap_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    uint32_t calculated_crc = crc32_calc(device->metadata_bitmap, device->superblock.metadata_bitmap_size_bytes);
    if (calculated_crc != device->page_crc.metadata_bitmap_crc) {
        return 0;
    }
    return 1;
}

int is_key_valid(uint32_t key_index)
{
    // Шаг 1: Проверяем базовые параметры
    if (!device || key_index >= device->key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    // Шаг 2: Проверяем флаг в key_index. Если ключ в процессе записи, он невалиден.
    if (device->key_index[key_index].flags == 2) {
        return 0;
    }
    // Шаг 3: Читаем с диска метаданные для этого ключа
    uint32_t metadata_offset = device->key_index[key_index].metadata_offset;
    kvs_metadata metadata;
    if (kvs_read_region(device->fp, metadata_offset, &metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    // Шаг 4: Проверяем, что ключ в метаданных на диске совпадает с ключом в key_index
    if (memcmp(metadata.key, device->key_index[key_index].key, KVS_KEY_SIZE) != 0) {
        return 0;
    }
    // Шаг 5: Выделяем буфер под данные и читаем их с диска
    uint32_t aligned_value_len = align_up(metadata.value_size, device->superblock.word_size_bytes);
    uint8_t *value_buffer = calloc(1, aligned_value_len);
    if (!value_buffer) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    if (kvs_read_region(device->fp, metadata.value_offset, value_buffer, aligned_value_len) < 0) {
        free(value_buffer);
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    // Шаг 6: Создаем единый буфер для проверки целостности.
    size_t total_size = sizeof(kvs_metadata) + aligned_value_len;
    uint8_t *check_buffer = calloc(1,total_size);
    if (!check_buffer) {
        free(value_buffer);
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    memcpy(check_buffer, &metadata, sizeof(kvs_metadata));
    memcpy(check_buffer + sizeof(kvs_metadata), value_buffer, aligned_value_len);

    // Шаг 7: Считаем CRC для этого единого буфера
    uint32_t calculated_crc = crc32_calc(check_buffer, total_size);
    // Шаг 8: Сравниваем вычисленный CRC с тем, что хранится в массиве entry_crc
    uint32_t slot_index = (metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
    bool is_valid = (calculated_crc == device->page_crc.entry_crc[slot_index]);

    // Шаг 9: Освобождаем всю выделенную память
    free(value_buffer);
    free(check_buffer);
    return is_valid ? 1 : 0;
}