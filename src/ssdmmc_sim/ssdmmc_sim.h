#ifndef SSDMMCSTORE_SSDMMC_SIM_H
#define SSDMMCSTORE_SSDMMC_SIM_H

#include "common.h"

typedef enum {
    SSDMMC_OK = 0,
    SSDMMC_ERR_INVALID_PAGE = -1,    // Неверный номер страницы
    SSDMMC_ERR_INVALID_OFFSET = -2,  // Неверное смещение слова
    SSDMMC_ERR_NULL_POINTER = -3,    // Передан NULL указатель на файл
    SSDMMC_ERR_SEEK_FAILED = -4,     // Ошибка позиционирования в файле
    SSDMMC_ERR_IO_FAILED = -5,       // Ошибка чтения/записи
    SSDMMC_ERR_MALLOC_FAILED = -6,   // Ошибка выделения памяти
    SSDMMC_ERR_MKDIR_FAILED = -7     // Ошибка создания директории
} ssdmmc_status_t;


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

// Проверяет существование директории для данных и создает ее, если она отсутствует.
// Возвращает SSDMMC_OK при успехе или SSDMMC_ERR_MKDIR_FAILED при ошибке.
int ssdmmc_sim_ensure_data_dir_exists(void);

// Возвращает константную строку с именем файла хранилища.
const char* ssdmmc_sim_get_storage_filename(void);

// Устанавливает обратный отсчет операций записи, после которого симулятор
// аварийно завершит работу, имитируя внезапное отключение питания.
// count - количество операций ssdmmc_sim_write_word, которые должны успешно
//         выполниться перед сбоем. Если count < 0, таймер отключается.
void ssdmmc_sim_set_write_failure_countdown(int count);


#endif