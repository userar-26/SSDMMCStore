#ifndef SSDMMCSTORE_KVS_VALID_H
#define SSDMMCSTORE_KVS_VALID_H

#include "kvs_types.h"
#include "kvs_metadata.h"
#include "kvs_internal_io.h"
#include "kvs_internal.h"


// Проверяет корректность битовой карты (bitmap) устройства.
// Возвращает 1, если битовая карта валидна, 0 в случае ошибки или несоответствия.
int is_bitmap_valid(void);

// Проверяет корректность массива счетчиков перезаписей страниц.
// Возвращает 1, если массив валиден, 0 в случае ошибки или несоответствия.
int is_page_rewrite_count_valid();

// Проверяет корректность единицы метаданных.
// metadata - указатель на структуру метаданных, которую требуется проверить
// Возвращает 1, если метаданные корректны, 0 в случае ошибки или несоответствия.
int is_metadata_entry_valid(const kvs_metadata *metadata);

// Проверяет корректность биткарты метаданных (metadata_bitmap) устройства.
// Возвращает 1, если битовая карта валидна, 0 в случае ошибки или несоответствия.
int is_metadata_bitmap_valid(void);

// Проверяет, является ли ключ по указанному индексу в key_index полностью валидным.
// Проверяет флаги, соответствие ключей и единый CRC для связки "метаданные + данные".
// key_index - индекс ключа в массиве device->key_index.
// Возвращает 1 если ключ валиден, 0 если нет, или отрицательный код ошибки.
int is_key_valid(uint32_t key_index);

#endif //SSDMMCSTORE_KVS_VALID_H