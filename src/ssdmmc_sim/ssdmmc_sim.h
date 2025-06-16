#ifndef SSDMMCSTORE_SSDMMC_SIM_H
#define SSDMMCSTORE_SSDMMC_SIM_H

#include "common.h"

#define SSDMMC_STORAGE_FILENAME "kvs_storage.bin"

// Читает одно слово из указанной страницы по смещению.
//
// fp         - указатель на открытый файл-эмулятор устройства.
// page_num   - номер страницы (от 0 до количества страниц - 1).
// word_offset- смещение слова внутри страницы (от 0 до слов на страницу - 1).
// word       - указатель на буфер, куда будет записано слово.
//
// Возвращает 0 при успехе, отрицательное значение при ошибке.
int ssdmmc_sim_read_word(FILE *fp, uint32_t page_num, uint32_t word_offset, void *word);

// Записывает одно слово в указанную страницу по смещению.
//
// fp         - указатель на открытый файл-эмулятор устройства.
// page_num   - номер страницы (от 0 до количества страниц - 1).
// word_offset- смещение слова внутри страницы (от 0 до слов на страницу - 1).
// word       - указатель на данные слова для записи.
//
// Возвращает 0 при успехе, отрицательное значение при ошибке.
int ssdmmc_sim_write_word(FILE *fp, uint32_t page_num, uint32_t word_offset, const void *word);

// Очищает (стирает) одну страницу в хранилище.
//
// fp         - указатель на открытый файл-эмулятор устройства.
// page_num   - номер страницы (от 0 до количества страниц - 1).
//
// Возвращает 0 при успехе, отрицательное значение при ошибке.
int ssdmmc_sim_erase_page(FILE *fp, uint32_t page_num);

// Полностью очищает (форматирует) все устройство.
int ssdmmc_sim_format(FILE *fp);

// Возвращает общее количество страниц в памяти.
uint32_t ssdmmc_sim_get_page_count(void);

// Возвращает количество слов в одной странице.
uint32_t ssdmmc_sim_get_words_per_page(void);

// Возвращает размер одного слова в байтах.
uint32_t ssdmmc_sim_get_word_size(void);


#endif
