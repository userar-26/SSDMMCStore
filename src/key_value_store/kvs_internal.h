#ifndef SSDMMCSTORE_KVS_INTERNAL_H
#define SSDMMCSTORE_KVS_INTERNAL_H


#include "kvs_types.h"

// Внутренние, детальные коды состояния.
// Используются только внутри библиотеки для точной диагностики ошибок.
typedef enum {

    KVS_INTERNAL_OK = 0,

    // Ошибки параметров и состояния
    KVS_INTERNAL_ERR_NULL_DEVICE = -1,
    KVS_INTERNAL_ERR_NULL_PARAM = -2,
    KVS_INTERNAL_ERR_INVALID_PARAM = -3,

    // Ошибки ввода/вывода (I/O)
    KVS_INTERNAL_ERR_READ_FAILED = -6,
    KVS_INTERNAL_ERR_WRITE_FAILED = -7,
    KVS_INTERNAL_ERR_ERASE_FAILED = -8,
    KVS_INTERNAL_ERR_FILE_OPEN_FAILED = -9,

    // Ошибки распределения ресурсов
    KVS_INTERNAL_ERR_MALLOC_FAILED = -11,
    KVS_INTERNAL_ERR_NO_FREE_METADATA_SPACE = -12,
    KVS_INTERNAL_ERR_KEY_INDEX_FULL = -14,

    // Ошибки целостности и повреждения данных
    KVS_INTERNAL_ERR_CORRUPT_SUPERBLOCK = -15,


} kvs_internal_status;


// Освобождает всю память, выделенную под устройство, и закрывает файл.
void kvs_free_device();

// Записывает сообщение в лог-файл библиотеки KVS.
void kvs_log(const char *format, ...);

// Выравнивает значение size вверх до ближайшего кратного align.
uint32_t align_up(uint32_t size, uint32_t align);


#endif //SSDMMCSTORE_KVS_INTERNAL_H