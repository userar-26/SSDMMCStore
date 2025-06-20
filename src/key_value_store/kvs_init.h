#ifndef SSDMMCSTORE_KVS_INIT_H
#define SSDMMCSTORE_KVS_INIT_H

#include "kvs_internal.h"
#include "kvs_metadata.h"
#include "kvs_internal_io.h"


// Создает новое хранилище: выделяет память, рассчитывает параметры, форматирует файл, записывает superblock и инициализирует служебные структуры.
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_init_new(size_t storage_size_bytes);

// Загружает и валидирует существующее хранилище: открывает файл, читает superblock, выделяет память, валидирует и восстанавливает служебные структуры.
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_load_existing(void);

// Заполняет поля superblock и выделяет память под служебные массивы.
// Возвращает 0 при успехе, отрицательное значение при ошибке.
kvs_internal_status kvs_setup_device(size_t storage_size_bytes);

#endif //SSDMMCSTORE_KVS_INIT_H
