#include "kvs_valid.h"

int is_superblock_valid(const kvs_device *dev)
{
    if (!dev)
        return -1;

    if (dev->superblock.magic != KVS_SUPERBLOCK_MAGIC)
        return -2;

    uint8_t *data = malloc(KVS_SUPERBLOCK_SIZE);
    if (!data)
        return -3;

    // Считываем образ суперблока из файла устройства
    if (kvs_read_region(dev->fp, 0, data, KVS_SUPERBLOCK_SIZE) < 0) {
        free(data);
        return -4;
    }

    uint32_t temp_crc = crc32_calc(data, KVS_SUPERBLOCK_SIZE);
    free(data);

    if (temp_crc != dev->page_crc.superblock_crc) {
        return -5;
    }

    return 1;
}

int is_bitmap_valid(const kvs_device *dev)
{
    if (!dev)
        return -1;

    uint8_t *data = malloc(dev->superblock.bitmap_size_bytes);
    if (!data)
        return -3;

    if (kvs_read_region(dev->fp, dev->superblock.bitmap_offset, data, dev->superblock.bitmap_size_bytes) < 0) {
        free(data);
        return -4;
    }

    uint32_t temp_crc = crc32_calc(data, dev->superblock.bitmap_size_bytes);
    free(data);

    if (temp_crc != dev->page_crc.bitmap_crc) {
        return 0;
    }

    return 1;
}

int is_page_rewrite_count_valid(const kvs_device *dev)
{
    if (!dev)
        return -1;

    uint32_t data_size = dev->superblock.page_crc_offset - dev->superblock.page_rewrite_offset;

    uint8_t *data = malloc(data_size );
    if (!data)
        return -3;

    if (kvs_read_region(dev->fp, dev->superblock.page_rewrite_offset , data, data_size ) < 0) {
        free(data);
        return -4;
    }

    uint32_t temp_crc = crc32_calc(data, data_size);
    free(data);

    if (temp_crc != dev->page_crc.rewrite_crc) {
        return 0;
    }

    return 1;
}

int is_metadata_valid(const kvs_device *dev)
{
    if (!dev)
        return -1;

    uint8_t *data = malloc(dev->superblock.metadata_size_bytes);
    if (!data)
        return -3;

    if (kvs_read_region(dev->fp, dev->superblock.metadata_offset, data, dev->superblock.metadata_size_bytes) < 0) {
        free(data);
        return -4;
    }

    uint32_t temp_crc = crc32_calc(data, dev->superblock.metadata_size_bytes);
    free(data);

    if (temp_crc != dev->page_crc.metadata_crc) {
        return 0;
    }

    return 1;
}

int is_userdata_valid(uint32_t value_offset, uint32_t value_size)
{
    if (!device)
        return -1;

    uint32_t data_offset = device->superblock.metadata_offset - device->superblock.userdata_size_bytes;

    // Определим первую и последнюю страницу, в которые попадает user data
    uint32_t first_page = (value_offset + data_offset) / device->superblock.page_size_bytes;
    uint32_t last_page = (value_offset + data_offset + value_size - 1) / device->superblock.page_size_bytes;

    uint8_t *page = malloc(device->superblock.page_size_bytes);
    if (!page)
        return -2;

    for (uint32_t i = first_page; i <= last_page; i++)
    {
        // Смещением выбираем нужную страницу
        uint32_t page_offset = data_offset + i * device->superblock.page_size_bytes;
        if (kvs_read_region(device->fp, page_offset, page, device->superblock.page_size_bytes) < 0)
        {
            free(page);
            return -3;
        }
        uint32_t temp_crc = crc32_calc(page, device->superblock.page_size_bytes);

        if (temp_crc != device->page_crc.page_crc[i])
        {
            free(page);
            return 0;  // Не валидно
        }
    }
    free(page);
    return 1; // Все crc совпали, страница валидна
}