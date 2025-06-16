#include "kvs_internal.h"

int kvs_write_region(FILE *fp, uint32_t offset, const void *data, uint32_t size)
{
    if (!device)
        return -1;

    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;

    // Проверка: offset и size должны быть кратны word_size
    if (offset % word_size != 0 || size % word_size != 0)
        return -2;

    const uint8_t *src = (const uint8_t*)data;
    uint32_t words_to_write = size / word_size;
    uint32_t cur_word = (offset / word_size) % words_per_page;
    uint32_t cur_page = offset / page_size;

    for (uint32_t i = 0; i < words_to_write; i++) {
        // Запись слова
        if (ssdmmc_sim_write_word(fp, cur_page, cur_word, src) < 0)
            return -3;

        src += word_size;
        cur_word++;
        // Если дошли до конца страницы — переходим на следующую
        if (cur_word == words_per_page) {
            cur_word = 0;
            cur_page++;
        }
    }

    return 0;
}

int kvs_read_region(FILE *fp, uint32_t offset, void *data, uint32_t size)
{
    if (!device)
        return -1;

    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;

    // Проверка: offset и size должны быть кратны word_size
    if (offset % word_size != 0 || size % word_size != 0)
        return -2;

    uint8_t *dst = (uint8_t*)data;
    uint32_t words_to_read = size / word_size;
    uint32_t cur_word = (offset / word_size) % words_per_page;
    uint32_t cur_page = offset / page_size;

    for (uint32_t i = 0; i < words_to_read; i++) {
        if (ssdmmc_sim_read_word(fp, cur_page, cur_word, dst) < 0)
            return -3;

        dst += word_size;
        cur_word++;
        if (cur_word == words_per_page) {
            cur_word = 0;
            cur_page++;
        }
    }

    return 0;
}

int kvs_clear_region(FILE *fp, uint32_t offset, uint32_t size)
{
    if (!device)
        return -1;

    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;

    uint32_t start = offset;
    uint32_t end = offset + size;

    while (start < end) {
        uint32_t cur_page = start / page_size;
        uint32_t page_start_offset = cur_page * page_size;
        uint32_t page_end_offset = page_start_offset + page_size;

        // Границы в текущей странице, которые нужно очистить
        uint32_t clear_start = (start > page_start_offset) ? (start - page_start_offset) : 0;
        uint32_t clear_end = (end < page_end_offset) ? (end - page_start_offset) : page_size;

        // 1. Прочитать всю страницу в буфер
        uint8_t *page_buf = malloc(page_size);
        if (!page_buf)
            return -2;

        for (uint32_t i = 0; i < words_per_page; i++) {
            if (ssdmmc_sim_read_word(fp, cur_page, i, page_buf + i * word_size) < 0) {
                free(page_buf);
                return -3;
            }
        }

        // 2. Очистить нужный регион в буфере (заполнить 0xFF)
        memset(page_buf + clear_start, 0xFF, clear_end - clear_start);

        // 3. Стереть страницу на устройстве
        if (ssdmmc_sim_erase_page(fp, cur_page) < 0) {
            free(page_buf);
            return -4;
        }

        // 4. Записать всю страницу обратно
        for (uint32_t i = 0; i < words_per_page; ++i) {
            if (ssdmmc_sim_write_word(fp, cur_page, i, page_buf + i * word_size) < 0) {
                free(page_buf);
                return -5;
            }
        }

        free(page_buf);

        // Переходим к следующей странице
        start = page_end_offset;
    }

    return 0;
}

int kvs_read_crc_info(FILE *fp, uint32_t offset, kvs_crc_info *crc_info, uint32_t page_count) {
    uint32_t cur = offset;

    if (kvs_read_region(fp, cur, &crc_info->superblock_crc, sizeof(crc_info->superblock_crc)) < 0)
        return -1;
    cur += sizeof(crc_info->superblock_crc);

    if (kvs_read_region(fp, cur, &crc_info->bitmap_crc, sizeof(crc_info->bitmap_crc)) < 0)
        return -2;
    cur += sizeof(crc_info->bitmap_crc);

    if (kvs_read_region(fp, cur, &crc_info->metadata_crc, sizeof(crc_info->metadata_crc)) < 0)
        return -3;
    cur += sizeof(crc_info->metadata_crc);

    if (kvs_read_region(fp, cur, &crc_info->rewrite_crc, sizeof(crc_info->rewrite_crc)) < 0)
        return -4;
    cur += sizeof(crc_info->rewrite_crc);

    // Читаем массив CRC для страниц
    if (kvs_read_region(fp, cur, crc_info->page_crc, page_count * sizeof(*crc_info->page_crc)) < 0)
        return -5;

    return 0;
}

int kvs_write_crc_info(FILE *fp, uint32_t offset, const kvs_crc_info *crc_info, uint32_t page_count) {
    uint32_t cur = offset;

    if (kvs_write_region(fp, cur, &crc_info->superblock_crc, sizeof(crc_info->superblock_crc)) < 0)
        return -1;
    cur += sizeof(crc_info->superblock_crc);

    if (kvs_write_region(fp, cur, &crc_info->bitmap_crc, sizeof(crc_info->bitmap_crc)) < 0)
        return -2;
    cur += sizeof(crc_info->bitmap_crc);

    if (kvs_write_region(fp, cur, &crc_info->metadata_crc, sizeof(crc_info->metadata_crc)) < 0)
        return -3;
    cur += sizeof(crc_info->metadata_crc);

    if (kvs_write_region(fp, cur, &crc_info->rewrite_crc, sizeof(crc_info->rewrite_crc)) < 0)
        return -4;
    cur += sizeof(crc_info->rewrite_crc);

    if (kvs_write_region(fp, cur, crc_info->page_crc, page_count * sizeof(*crc_info->page_crc)) < 0)
        return -5;

    return 0;
}

int is_data_region_empty(uint32_t data_offset, size_t data_size)
{
    if (!device)
        return -1;
    if (data_size == 0)
        return -2;

    uint8_t *buf = malloc(data_size);
    if (!buf)
        return -3;

    if (kvs_read_region(device->fp, data_offset, buf, data_size) < 0) {
        free(buf);
        return -4;
    }

    for (size_t i = 0; i < data_size; ++i) {
        if (buf[i] != 0xFF) {
            free(buf);
            return 0;
        }
    }

    free(buf);
    return 1;
}


