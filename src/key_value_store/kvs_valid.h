//
// Created by userar on 14.06.2025.
//

#ifndef SSDMMCSTORE_KVS_VALID_H
#define SSDMMCSTORE_KVS_VALID_H

#include "kvs_types.h"
#include "kvs_metadata.h"
#include "kvs_internal_io.h"
#include "kvs_internal.h"

// Проверяет корректность суперблока устройства.
// device - указатель на структуру устройства
// Возвращает 1, если суперблок валиден, 0 в случае ошибки или несоответствия.
int is_superblock_valid(const kvs_device *device);

// Проверяет корректность битовой карты (bitmap) устройства.
// device - указатель на структуру устройства
// Возвращает 1, если битовая карта валидна, 0 в случае ошибки или несоответствия.
int is_bitmap_valid(const kvs_device *device);

// Проверяет корректность массива счетчиков перезаписей страниц.
// device - указатель на структуру устройства
// Возвращает 1, если массив валиден, 0 в случае ошибки или несоответствия.
int is_page_rewrite_count_valid(const kvs_device *device);

// Проверяет корректность всей области метаданных устройства.
// device - указатель на структуру устройства
// Возвращает 1, если область метаданных валидна, 0 в случае ошибки или несоответствия.
int is_metadata_valid(const kvs_device *device);

// Проверяет корректность единицы метаданных.
// metadata - указатель на структуру метаданных, которую требуется проверить
// Возвращает 1, если метаданные корректны, 0 в случае ошибки или несоответствия.
int is_metadata_entry_valid(const kvs_metadata *metadata);

// Проверяет, что пользовательские данные по заданному смещению и размеру корректны.
// Возвращает 1, если данные валидны, 0 если нет.
int is_userdata_valid(uint32_t value_offset, uint32_t value_size);

// Проверяет, что ключ, находящийся по индексу key_index в массиве key_index.
// Возвращает 1, если валиден, 0 если нет.
int is_key_valid(uint32_t key_index);

#endif //SSDMMCSTORE_KVS_VALID_H
