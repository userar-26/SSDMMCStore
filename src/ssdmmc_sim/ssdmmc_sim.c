#include "ssdmmc_sim_internal.h"

int ssdmmc_sim_read_word(FILE *fp, uint32_t page_num, uint32_t word_offset, void *word)
{
    // Проверяем не выходят ли запрашиваемые данные за размер хранилища
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return -1;
    if (word_offset >= SSDMMC_SIM_WORDS_PER_PAGE)
        return -2;

    // Проверяем указатель на файл
    if (fp == NULL)
        return -3;

    // Вычисляем позицию необходимого слова
    uint32_t pos = (page_num * SSDMMC_SIM_WORDS_PER_PAGE + word_offset) * SSDMMC_SIM_WORD_SIZE;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return -4;
    }

    // Считываем слово
    size_t read = fread(word, 1, SSDMMC_SIM_WORD_SIZE, fp);

    // Проверяем считались ли запрашиваемые данные
    if (read != SSDMMC_SIM_WORD_SIZE)
        return -5;

    return 0;
}

int ssdmmc_sim_write_word(FILE *fp, uint32_t page_num, uint32_t word_offset, const void *word)
{
    // Проверяем не выходят ли запрашиваемые данные за размер хранилища
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return -1;
    if (word_offset >= SSDMMC_SIM_WORDS_PER_PAGE)
        return -2;

    // Проверяем указатель на файл
    if (fp == NULL)
        return -3;

    // Вычисляем позицию необходимого слова
    uint32_t pos = (page_num * SSDMMC_SIM_WORDS_PER_PAGE + word_offset) * SSDMMC_SIM_WORD_SIZE;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return -4;
    }

    // Записываем слово
    size_t written = fwrite(word, 1, SSDMMC_SIM_WORD_SIZE, fp);

    // Проверяем записались ли данные
    if (written != SSDMMC_SIM_WORD_SIZE)
        return -5;

    fflush(fp);
    return 0;
}

int ssdmmc_sim_erase_page(FILE *fp, uint32_t page_num)
{
    // Проверяем не выходит ли страница за количество страниц
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return -1;

    // Проверяем указатель на файл
    if (fp == NULL)
        return -2;

    // Вычисляем позицию необходимой страницы
    size_t page_size = SSDMMC_SIM_WORDS_PER_PAGE * SSDMMC_SIM_WORD_SIZE;
    uint32_t pos = page_num * page_size;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return -3;
    }

    // Выделяем буфер очистки и заполняем его
    uint8_t *erase_buf = malloc(page_size);
    if (erase_buf == NULL) {
        return -4;
    }
    memset(erase_buf, 0xFF, page_size);

    // Очищаем страницу
    size_t written = fwrite(erase_buf, 1, page_size, fp);
    free(erase_buf);

    // Проверяем записались ли данные
    if (written != page_size)
        return -5;

    fflush(fp);
    return 0;
}

int ssdmmc_sim_format(FILE *fp){

    // Проверяем указатель на файл
    if(fp == NULL)
        return -1;

    // Вычисляем размер хранилища
    uint32_t storage_size_bytes = SSDMMC_SIM_PAGE_COUNT * SSDMMC_SIM_WORD_SIZE * SSDMMC_SIM_WORDS_PER_PAGE;

    // Заполняем биты хранилища значением 0xFF(11111111) блоками по 4096 байт
    const size_t buf_size = 4096;
    uint8_t buf[buf_size];
    memset(buf, 0xFF, buf_size);

    size_t bytes_left = storage_size_bytes;
    while (bytes_left > 0) {
        size_t chunk = bytes_left < buf_size ? bytes_left : buf_size;
        size_t written = fwrite(buf, 1, chunk, fp);
        // Проверяем записалось ли нужное количество байтов
        if (written != chunk) {
            return -2;
        }
        bytes_left -= written;
    }

    fflush(fp);
    return 0;
}
