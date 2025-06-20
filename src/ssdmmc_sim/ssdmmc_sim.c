#include "ssdmmc_sim_internal.h"

int ssdmmc_sim_read_word(FILE *fp, uint32_t page_num, uint32_t word_offset, void *word)
{
    // Проверяем не выходят ли запрашиваемые данные за размер хранилища
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return SSDMMC_ERR_INVALID_PAGE;
    if (word_offset >= SSDMMC_SIM_WORDS_PER_PAGE)
        return SSDMMC_ERR_INVALID_OFFSET;

    // Проверяем указатель на файл
    if (fp == NULL)
        return SSDMMC_ERR_NULL_POINTER;

    // Вычисляем позицию необходимого слова
    uint32_t pos = (page_num * SSDMMC_SIM_WORDS_PER_PAGE + word_offset) * SSDMMC_SIM_WORD_SIZE;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return SSDMMC_ERR_SEEK_FAILED;
    }

    // Считываем слово
    size_t read = fread(word, 1, SSDMMC_SIM_WORD_SIZE, fp);

    // Проверяем считались ли запрашиваемые данные
    if (read != SSDMMC_SIM_WORD_SIZE)
        return SSDMMC_ERR_IO_FAILED;

    return SSDMMC_OK;
}

int ssdmmc_sim_write_word(FILE *fp, uint32_t page_num, uint32_t word_offset, const void *word)
{
    // Проверяем не выходят ли запрашиваемые данные за размер хранилища
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return SSDMMC_ERR_INVALID_PAGE;
    if (word_offset >= SSDMMC_SIM_WORDS_PER_PAGE)
        return SSDMMC_ERR_INVALID_OFFSET;

    // Проверяем указатель на файл
    if (fp == NULL)
        return SSDMMC_ERR_NULL_POINTER;

    // Вычисляем позицию необходимого слова
    uint32_t pos = (page_num * SSDMMC_SIM_WORDS_PER_PAGE + word_offset) * SSDMMC_SIM_WORD_SIZE;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return SSDMMC_ERR_SEEK_FAILED;
    }

    // Записываем слово
    size_t written = fwrite(word, 1, SSDMMC_SIM_WORD_SIZE, fp);

    // Проверяем записались ли данные
    if (written != SSDMMC_SIM_WORD_SIZE)
        return SSDMMC_ERR_IO_FAILED;

    fflush(fp);
    return SSDMMC_OK;
}

int ssdmmc_sim_erase_page(FILE *fp, uint32_t page_num)
{
    // Проверяем не выходит ли страница за количество страниц
    if (page_num >= SSDMMC_SIM_PAGE_COUNT)
        return SSDMMC_ERR_INVALID_PAGE;

    // Проверяем указатель на файл
    if (fp == NULL)
        return SSDMMC_ERR_NULL_POINTER;

    // Вычисляем позицию необходимой страницы
    size_t page_size = SSDMMC_SIM_WORDS_PER_PAGE * SSDMMC_SIM_WORD_SIZE;
    uint32_t pos = page_num * page_size;

    // Переходим в вычисленную позицию
    if (fseeko(fp, pos, SEEK_SET) != 0) {
        return SSDMMC_ERR_SEEK_FAILED;
    }

    // Выделяем буфер очистки и заполняем его
    uint8_t *erase_buf = calloc(1,page_size);
    if (erase_buf == NULL) {
        return SSDMMC_ERR_MALLOC_FAILED;
    }
    memset(erase_buf, 0xFF, page_size);

    // Очищаем страницу
    size_t written = fwrite(erase_buf, 1, page_size, fp);
    free(erase_buf);

    // Проверяем записались ли данные
    if (written != page_size)
        return SSDMMC_ERR_IO_FAILED;

    fflush(fp);
    return SSDMMC_OK;
}

int ssdmmc_sim_format(FILE *fp){
    // Проверяем указатель на файл
    if(fp == NULL)
        return SSDMMC_ERR_NULL_POINTER;

    // Убедимся, что запись будет с самого начала
    rewind(fp);

    // Вычисляем размер хранилища
    uint32_t storage_size_bytes = SSDMMC_SIM_PAGE_COUNT * SSDMMC_SIM_WORD_SIZE * SSDMMC_SIM_WORDS_PER_PAGE;

    // Заполняем биты хранилища значением 0xFF блоками по 4096 байт
    const size_t buf_size = 4096;
    uint8_t buf[buf_size];
    memset(buf, 0xFF, buf_size);

    size_t bytes_left = storage_size_bytes;
    while (bytes_left > 0) {
        size_t chunk = bytes_left < buf_size ? bytes_left : buf_size;
        size_t written = fwrite(buf, 1, chunk, fp);
        // Проверяем записалось ли нужное количество байтов
        if (written != chunk) {
            return SSDMMC_ERR_IO_FAILED;
        }
        bytes_left -= written;
    }

    fflush(fp);
    return SSDMMC_OK;
}