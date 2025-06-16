
#include "kvs_metadata.h"
#include "kvs_internal_io.h"

uint32_t crc32_calc(const void *data, size_t size)
{
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < size; ++i) {
        crc ^= buf[i];
        for (int j = 0; j < 8; ++j) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc = crc >> 1;
        }
    }
    return ~crc;
}

int crc_update_region(uint32_t offset, uint32_t size)
{
    if (!device)
        return -1;

    if ( offset < device->superblock.data_offset)
        return -2;

    if ( offset >= device->superblock.metadata_offset)
        return -3;

    if (offset + size > device->superblock.storage_size_bytes)
        return -3;

    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t first_page = offset / page_size;
    uint32_t last_page = (offset + size - 1) / page_size;
    uint32_t data_offset = device->superblock.data_offset + first_page * page_size;

    uint8_t *temp_page = malloc(page_size);
    if (!temp_page)
        return -4;

    for (uint32_t i = first_page; i <= last_page; i++) {
        if (kvs_read_region(device->fp, data_offset, temp_page, page_size) < 0) {
            free(temp_page);
            return -5;
        }

        uint32_t temp_crc = crc32_calc(temp_page, page_size);

        device->page_crc.page_crc[i] = temp_crc;

        data_offset += page_size;
    }

    free(temp_page);
    return 0;
}

int rewrite_count_increment_region(uint32_t offset, uint32_t size)
{
    if (!device)
        return -1;

    if (offset < device->superblock.data_offset)
        return -2;

    if (offset + size > device->superblock.storage_size_bytes)
        return -3;

    if (size == 0)
        return -4;

    uint32_t first_page = offset / device->superblock.page_size_bytes;
    uint32_t last_page  = (offset + size - 1) / device->superblock.page_size_bytes;

    for (uint32_t i = first_page; i <= last_page; i++) {
        device->page_rewrite_count[i]++;
    }

    return 0;
}

int bitmap_set_region(uint32_t offset, uint32_t size)
{
    if (!device)
        return -1;

    if (offset < device->superblock.data_offset)
        return -2;

    if (offset + size > device->superblock.storage_size_bytes)
        return -3;

    if (size == 0)
        return -4;

    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t first_page = offset / page_size;
    uint32_t last_page  = (offset + size - 1) / page_size;

    for (uint32_t i = first_page; i <= last_page; i++) {
        uint32_t byte_index = i / 8;
        uint8_t  bit_index  = i % 8;
        device->bitmap[byte_index] |= (1 << bit_index);
    }

    return 0;
}

int bitmap_clear_region(uint32_t offset, uint32_t size)
{
    if (!device)
        return -1;

    if (offset < device->superblock.data_offset)
        return -2;

    if (offset + size > device->superblock.storage_size_bytes)
        return -3;

    if (size == 0)
        return -4;

    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t first_page = offset / page_size;
    uint32_t last_page  = (offset + size - 1) / page_size;

    for (uint32_t i = first_page; i <= last_page; i++) {
        uint32_t byte_index = i / 8;
        uint8_t  bit_index  = i % 8;
        device->bitmap[byte_index] &= ~(1 << bit_index);
    }

    return 0;
}

int kvs_bitmap_create()
{
    if(!device)
        return -1;
    if ( bitmap_clear_region( (device->superblock.metadata_offset - device->superblock.userdata_size_bytes),  device->superblock.userdata_size_bytes) < 0)
        return -2;
    if (device->key_count == 0)
        return 0;

    kvs_metadata temp;

    for(int i = 0; i < device->key_count; i++)
    {
        if (bitmap_set_region( device->key_index[i].metadata_offset,sizeof(kvs_metadata) ) < 0)
            return -4;
        if (kvs_read_region(device->fp,device->key_index[i].metadata_offset,&temp,sizeof(kvs_metadata)) < 0 )
            return -5;
        if(bitmap_set_region( temp.value_offset,temp.value_size ) < 0)
            return -6;
    }

    return 0;
}

int build_key_index() {

    if(!device)
        return -1;

    device->key_count = 0;
    uint32_t cur_position = device->superblock.metadata_offset;
    kvs_metadata temp;
    for (int i = 0; i < device->superblock.max_key_count; i++) {
        if (kvs_read_region(device->fp, cur_position, &temp, sizeof(temp)) < 0) {
            kvs_log("Ошибка: не удалось считать метаданные из файла при построении key_index (позиция %u)", cur_position);
            return -2;
        }
        if (is_metadata_entry_valid(&temp)) {
            kvs_add_metadata_entry(&temp);
        }
        cur_position += sizeof(temp);
    }
    return 0;
}

int kvs_key_index_entry_cmp(const void *a, const void *b) {
    const kvs_key_index_entry *tempa = (const kvs_key_index_entry *)a;
    const kvs_key_index_entry *tempb = (const kvs_key_index_entry *)b;
    return memcmp(tempa->key, tempb->key, KVS_KEY_SIZE);
}
