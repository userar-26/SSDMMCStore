

#ifndef SSDMMCSTORE_KVS_TYPES_H
#define SSDMMCSTORE_KVS_TYPES_H

#include "kvs.h"
#include "../ssdmmc_sim/ssdmmc_sim.h"

#define KVS_MIN_NUM_METADATA      16
#define KVS_KEY_SIZE              128
#define KVS_SUPERBLOCK_SIZE       256
#define KVS_SUPERBLOCK_MAGIC      122221
#define KVS_LOG_FILENAME          "../kvs_log.txt"
#define KVS_STORAGE_FILENAME      "../data/kvs_storage.bin"

typedef struct {
    uint32_t superblock_crc;         // CRC для суперблока
    uint32_t bitmap_crc;             // CRC для битовой карты
    uint32_t metadata_crc;           // CRC для области метаданных
    uint32_t rewrite_crc;            // CRC для области очистки страниц
    uint32_t *page_crc;              // Массив CRC для каждой страницы пользовательских данных
} kvs_crc_info;

typedef struct {
    uint8_t  key[KVS_KEY_SIZE];      // Ключ
    uint32_t metadata_offset;        // Смещение метаданных для этого ключа
    uint8_t  flags;                  // Флаги (валидность, удаленность и т.д.)
} kvs_key_index_entry;


typedef struct {

    uint32_t magic;                  // Магическое число

    // Параметры устройства
    uint32_t storage_size_bytes;     // Физический размер устройства (байты)
    uint32_t userdata_size_bytes;    // Размер хранилища данных пользователя в байтах
    uint32_t global_page_count;      // Количество страниц всего хранилища
    uint16_t page_size_bytes;        // Размер страницы (байты)
    uint16_t words_per_page;         // Количество слов в странице
    uint8_t  word_size_bytes;        // Размер слова (байты)

    // Смещения служебных областей
    uint32_t bitmap_offset;          // Смещение битовой карты
    uint32_t page_rewrite_offset;    // Смещение массива очистки
    uint32_t page_crc_offset;        // Смещение массива CRC всех страниц
    uint32_t data_offset;            // Смещение пользовательских данных
    uint32_t metadata_offset;        // Смещение области метаданных

    // Размеры служебных областей
    uint32_t userdata_page_count;    // Количество страниц для данных пользователя
    uint16_t superblock_size_bytes;  // Размер суперблока в байтах
    uint16_t metadata_size_bytes;    // Размер единицы метаданных в байтах
    uint16_t bitmap_size_bytes;      // Размер битовой карты в байтах

    uint32_t max_key_count;          // Максимально возможное количество ключей

} kvs_superblock;

typedef struct {

    FILE *fp;                        // Указатель на файл-эмулятор

    kvs_superblock superblock;       // Суперблок нашего файла-эмулятора
    kvs_crc_info   page_crc;         // Структура для хранения crc-кодов, различных частей хранилища

    // Указатели на служебные области
    uint8_t  *bitmap;                // Битовая карта занятости страниц
    uint32_t *page_rewrite_count;    // Счетчики перезаписей страниц
    kvs_key_index_entry *key_index;  // Массив записей индекса ключей: для каждого ключа хранится его имя и смещение метаданных.

    uint32_t key_count;              // Текущее количество ключей

} kvs_device;


typedef struct {

    uint8_t  key[KVS_KEY_SIZE];      // Ключ (строка или бинарные данные)
    uint32_t value_offset;           // Смещение значения в файле
    uint32_t value_size;             // Размер значения в байтах
    //uint8_t  flags;                  // Флаги (валидность, удаленность и т.д.)
    //uint8_t  reserved[3];            // Резерв для выравнивания

} kvs_metadata;

extern kvs_device * device;

#endif //SSDMMCSTORE_KVS_TYPES_H
