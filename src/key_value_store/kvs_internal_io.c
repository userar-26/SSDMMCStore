#include "kvs_internal.h"
#include "kvs_internal_io.h"

kvs_internal_status kvs_write_region(FILE *fp, uint32_t offset, const void *data, uint32_t size)
{
    // Шаг 1: Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;

    // Проверяем, что смещение и размер выровнены по размеру слова
    if (offset % word_size != 0 || size % word_size != 0) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Шаг 2: Вычисляем начальные координаты для записи
    const uint8_t *src      = data;
    uint32_t words_to_write = size / word_size;
    uint32_t cur_word       = (offset / word_size) % words_per_page;
    uint32_t cur_page       = offset / (words_per_page * word_size);

    // Шаг 3: В цикле записываем каждое слово
    for (uint32_t i = 0; i < words_to_write; i++) {
        if (ssdmmc_sim_write_word(fp, cur_page, cur_word, src) < 0) {
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }

        // Перемещаем указатель на незаписанные данные
        src += word_size;
        cur_word++;

        // Если дошли до конца страницы, переходим на следующую
        if (cur_word == words_per_page) {
            cur_word = 0;
            cur_page++;
        }
    }

    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_read_region(FILE *fp, uint32_t offset, void *data, uint32_t size)
{
    // Шаг 1: Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;

    // Проверяем выравнивание
    if (offset % word_size != 0 || size % word_size != 0) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Шаг 2: Вычисляем начальные координаты для чтения
    uint8_t *dst           = data;
    uint32_t words_to_read = size / word_size;
    uint32_t cur_word      = (offset / word_size) % words_per_page;
    uint32_t cur_page      = offset / (words_per_page * word_size);

    // Шаг 3: В цикле считываем каждое слово
    for (uint32_t i = 0; i < words_to_read; i++) {
        if (ssdmmc_sim_read_word(fp, cur_page, cur_word, dst) < 0) {
            return KVS_INTERNAL_ERR_READ_FAILED;
        }

        // Перемещаем указатель буфера на незаписанный блок
        dst += word_size;
        cur_word++;

        // Если дошли до конца страницы, переходим на следующую
        if (cur_word == words_per_page) {
            cur_word = 0;
            cur_page++;
        }
    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_clear_region(FILE *fp, uint32_t offset, uint32_t size)
{
    // Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    uint32_t page_size      = device->superblock.page_size_bytes;
    uint32_t words_per_page = device->superblock.words_per_page;
    uint32_t word_size      = device->superblock.word_size_bytes;

    uint32_t start = offset;
    uint32_t end   = offset + size;

    // Цикл работает, пока мы не очистим весь запрошенный регион
    while (start < end) {

        // Шаг 1: Определяем, с какой страницей мы работаем на этой итерации
        uint32_t cur_page          = start / page_size;
        uint32_t page_start_offset = cur_page * page_size;
        uint32_t page_end_offset   = page_start_offset + page_size;

        // Шаг 2: Определяем границы очистки ВНУТРИ текущей страницы
        uint32_t clear_start = (start > page_start_offset) ? (start - page_start_offset) : 0;
        uint32_t clear_end   = (end < page_end_offset) ? (end - page_start_offset) : page_size;

        // Шаг 3: Выполняем цикл
        uint8_t *page_buf = calloc(1, page_size);
        if (!page_buf) {
            return KVS_INTERNAL_ERR_MALLOC_FAILED;
        }

        // 3.1. Считываем всю страницу в буфер, чтобы не потерять данные, которые не нужно стирать
        for (uint32_t i = 0; i < words_per_page; i++) {
            if (ssdmmc_sim_read_word(fp, cur_page, i, page_buf + i * word_size) < 0) {
                free(page_buf);
                return KVS_INTERNAL_ERR_READ_FAILED;
            }
        }

        // 3.2. Стираем нужную часть данных в буфере, заполняя ее 0xFF
        memset(page_buf + clear_start, 0xFF, clear_end - clear_start);

        // 3.3. Стираем всю физическую страницу на устройстве
        if (ssdmmc_sim_erase_page(fp, cur_page) < 0) {
            free(page_buf);
            return KVS_INTERNAL_ERR_ERASE_FAILED;
        }

        // 3.4. Записываем измененный буфер обратно на только что очищенную страницу
        for (uint32_t i = 0; i < words_per_page; i++) {
            if (ssdmmc_sim_write_word(fp, cur_page, i, page_buf + i * word_size) < 0) {
                free(page_buf);
                return KVS_INTERNAL_ERR_WRITE_FAILED;
            }
        }

        free(page_buf);

        // Шаг 4: Переходим к началу следующей страницы для следующей итерации
        start = page_end_offset;
    }

    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_read_crc_info(void) {
    // Шаг 1: Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    FILE *fp               = device->fp;
    uint32_t offset        = device->superblock.page_crc_offset;
    kvs_crc_info *crc_info = &device->page_crc;
    uint32_t key_count     = device->superblock.max_key_count;
    uint32_t cur           = offset;

    // Шаг 2: Последовательно читаем каждое поле структуры CRC
    if (kvs_read_region(fp, cur, &crc_info->superblock_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_read_region(fp, cur, &crc_info->superblock_backup_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_read_region(fp, cur, &crc_info->bitmap_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_read_region(fp, cur, &crc_info->rewrite_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_read_region(fp, cur, &crc_info->metadata_bitmap_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    cur += sizeof(uint32_t);

    // Шаг 3: Читаем единый массив CRC для всех записей
    if (kvs_read_region(fp, cur, crc_info->entry_crc, key_count * sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_write_crc_info(void) {

    // Шаг 1: Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    FILE *fp               = device->fp;
    uint32_t offset        = device->superblock.page_crc_offset;
    kvs_crc_info *crc_info = &device->page_crc;
    uint32_t key_count     = device->superblock.max_key_count;
    uint32_t cur           = offset;

    // Шаг 2: Последовательно записываем каждое поле структуры CRC
    if (kvs_write_region(fp, cur, &crc_info->superblock_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_write_region(fp, cur, &crc_info->superblock_backup_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_write_region(fp, cur, &crc_info->bitmap_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_write_region(fp, cur, &crc_info->rewrite_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    cur += sizeof(uint32_t);
    if (kvs_write_region(fp, cur, &crc_info->metadata_bitmap_crc, sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    cur += sizeof(uint32_t);

    // Шаг 3: Записываем единый массив CRC для всех записей
    if (kvs_write_region(fp, cur, crc_info->entry_crc, key_count * sizeof(uint32_t)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    return KVS_INTERNAL_OK;
}

int is_data_region_empty(uint32_t data_offset, size_t data_size)
{
    // Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (data_size == 0) {
        return 1;
    }

    uint8_t *buf = calloc(1, data_size);
    if (!buf) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    if (kvs_read_region(device->fp, data_offset, buf, data_size) < 0) {
        free(buf);
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    for (size_t i = 0; i < data_size; i++) {
        if (buf[i] != 0xFF) {
            free(buf);
            return 0;
        }
    }
    free(buf);
    return 1;
}