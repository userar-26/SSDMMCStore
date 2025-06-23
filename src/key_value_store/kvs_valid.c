#include "kvs_valid.h"
#include "kvs_internal_io.h"
#include "kvs_metadata.h"

int is_bitmap_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Считаем CRC для битовой карты, находящейся в ОЗУ, и сравниваем с прочитанным с диска CRC
    uint32_t temp_crc = crc32_calc(device->bitmap, device->superblock.bitmap_size_bytes);
    if (temp_crc != device->page_crc.bitmap_crc) {
        return 0;
    }

    return 1;
}

int is_page_rewrite_count_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Вычисляем точный размер области счетчиков
    uint32_t data_size = device->superblock.page_crc_offset - device->superblock.page_rewrite_offset;

    // Считаем CRC для данных в ОЗУ и сравниваем с прочитанным с диска CRC
    uint32_t temp_crc = crc32_calc(device->page_rewrite_count, data_size);
    if (temp_crc != device->page_crc.rewrite_crc) {
        return 0;
    }

    return 1;
}

int is_userdata_valid(uint32_t value_offset, uint32_t value_size)
{
    // Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Проверяем, что смещение и размер не выходят за границы области пользовательских данных
    if (value_offset < device->superblock.data_offset || value_offset + value_size > device->superblock.metadata_offset) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    // Выделяем временный буфер для чтения одной страницы
    uint8_t *page = calloc(1,device->superblock.page_size_bytes);
    if (!page) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    // Определяем, с какой физической страницы начинаются пользовательские данные
    uint32_t userdata_start_page = device->superblock.data_offset / device->superblock.page_size_bytes;

    // Вычисляем диапазон страниц для проверки
    uint32_t first_page = value_offset / device->superblock.page_size_bytes;
    uint32_t last_page = (value_offset + value_size - 1) / device->superblock.page_size_bytes;

    // В цикле проверяем CRC каждой страницы
    for (uint32_t i = first_page; i <= last_page; i++)
    {
        uint32_t page_offset = i * device->superblock.page_size_bytes;
        if (kvs_read_region(device->fp, page_offset, page, device->superblock.page_size_bytes) < 0) {
            free(page);
            return KVS_INTERNAL_ERR_READ_FAILED;
        }

        uint32_t temp_crc = crc32_calc(page, device->superblock.page_size_bytes);
        uint32_t crc_index = i - userdata_start_page;

        if (temp_crc != device->page_crc.page_crc[crc_index]) {
            free(page);
            return 0;
        }
    }
    free(page);
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
    // Используем выровненный размер, так как именно столько места было реально занято на диске
    uint32_t aligned_size = align_up(metadata->value_size, device->superblock.word_size_bytes);
    int empty_check = is_data_region_empty(metadata->value_offset, aligned_size);
    if (empty_check < 0) {
        // Возвращаем код ошибки, если чтение не удалось
        return empty_check;
    }
    if (empty_check == 1) {
        // Область пуста, значит, метаданные невалидны
        return 0;
    }

    // Проверяем целостность самих данных по CRC
    // Передаем выровненный размер, чтобы is_userdata_valid проверила все нужные страницы
    if (is_userdata_valid(metadata->value_offset, aligned_size) != 1) {
        return 0;
    }

    return 1;
}

int is_metadata_bitmap_valid()
{
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Считаем CRC для битовой карты в ОЗУ и сравниваем с прочитанным с диска CRC
    uint32_t temp_crc = crc32_calc(device->metadata_bitmap, device->superblock.metadata_bitmap_size_bytes);
    if (temp_crc != device->page_crc.metadata_bitmap_crc) {
        return 0;
    }

    return 1;
}

int is_metadata_slot_valid(uint32_t slot_index, kvs_metadata *metadata_out) {

    if (!device || slot_index >= device->superblock.max_key_count || !metadata_out) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Читаем слот с диска
    uint32_t slot_offset = device->superblock.metadata_offset + (slot_index * sizeof(kvs_metadata));
    if (kvs_read_region(device->fp, slot_offset, metadata_out, sizeof(kvs_metadata)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Считаем CRC и сравниваем с сохраненным
    uint32_t calculated_crc = crc32_calc(metadata_out, sizeof(kvs_metadata));
    if (calculated_crc != device->page_crc.metadata_slot_crc[slot_index]) {
        return 0;
    }

    return 1;
}

int is_key_valid(uint32_t key_index)
{

    if (!device || key_index >= device->key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Если флаг = 2, ключ невалиден (в процессе записи произошел сбой)
    if (device->key_index[key_index].flags == 2) {
        return 0;
    }

    // Проверяем целостность слота метаданных по CRC
    uint32_t moff       = device->key_index[key_index].metadata_offset;
    uint32_t slot_index = (moff - device->superblock.metadata_offset) / sizeof(kvs_metadata);
    kvs_metadata cur_metadata;
    int slot_valid_check = is_metadata_slot_valid(slot_index, &cur_metadata);
    if (slot_valid_check < 0) {
        return slot_valid_check;
    }
    if (slot_valid_check == 0) {
        return 0;
    }

    // Проверяем, что ключ в метаданных на диске совпадает с ключом в key_index
    if (memcmp(cur_metadata.key, device->key_index[key_index].key, KVS_KEY_SIZE) != 0) {
        return 0;
    }

    // Проверяем целостность самих пользовательских данных
    // Используем выровненный размер, чтобы гарантировать проверку всех страниц, затронутых при записи.
    uint32_t aligned_size = align_up(cur_metadata.value_size, device->superblock.word_size_bytes);
    if (is_userdata_valid(cur_metadata.value_offset, aligned_size) != 1) {
        return 0;
    }

    return 1;
}
