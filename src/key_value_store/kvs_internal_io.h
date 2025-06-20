#ifndef SSDMMCSTORE_KVS_INTERNAL_IO_H
#define SSDMMCSTORE_KVS_INTERNAL_IO_H

#include "kvs_types.h"
#include "kvs_internal.h"

// Считывает данные из региона файла.
// fp - указатель на открытый файл
// offset - смещение (в байтах) относительно начала файла, с которого начинается чтение
// data - указатель на буфер, куда будут считаны данные
// size - размер данных в байтах
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_read_region(FILE *fp, uint32_t offset, void *data, uint32_t size);

// Записывает данные в регион файла.
// fp - указатель на открытый файл
// offset - смещение (в байтах) относительно начала файла, с которого начинается запись
// data - указатель на буфер с данными для записи
// size - размер данных в байтах
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_write_region(FILE *fp, uint32_t offset, const void *data, uint32_t size);

// Очищает регион файла (заполняет 0xFF).
// fp - указатель на открытый файл
// offset - смещение (в байтах) относительно начала файла, с которого начинается очистка
// size - размер региона в байтах
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_clear_region(FILE *fp, uint32_t offset, uint32_t size);

// Проверяет, что область данных по заданному смещению пуста (заполнена 0xFF).
// Возвращает 1, если область пуста, 0 — если найдены отличные от 0xFF байты, отрицательное значение — код ошибки.
// Возможные причины ошибки: устройство не инициализировано, некорректные параметры, ошибка чтения.
kvs_internal_status is_data_region_empty(uint32_t data_offset, size_t data_size);

// Читает структуру CRC (служебные поля и массивы CRC страниц) из файла.
kvs_internal_status kvs_read_crc_info(void);

// Записывает всю структуру CRC (служебные поля и массивы CRC страниц) в файл.
kvs_internal_status kvs_write_crc_info(void);



#endif //SSDMMCSTORE_KVS_INTERNAL_IO_H
