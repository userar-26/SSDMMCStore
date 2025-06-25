#ifndef SSDMMCSTORE_KVS_H
#define SSDMMCSTORE_KVS_H

#include "common.h"

// Коды состояния KVS.
// Эти значения возвращаются пользовательскими функциями API.
typedef enum {
    KVS_SUCCESS = 0,                    // Операция успешно завершена
    KVS_ERROR_NOT_INITIALIZED = -1,     // Библиотека не была инициализирована
    KVS_ERROR_ALREADY_INITIALIZED = -2, // Библиотека уже была успешно инициализирована
    KVS_ERROR_INVALID_PARAM = -3,       // Передан некорректный параметр (например, NULL, неверный размер)
    KVS_ERROR_KEY_NOT_FOUND = -4,       // Указанный ключ не найден
    KVS_ERROR_KEY_ALREADY_EXISTS = -5,  // Ключ уже существует в хранилище
    KVS_ERROR_BUFFER_TOO_SMALL = -6,    // Буфер для чтения значения слишком мал
    KVS_ERROR_NO_SPACE = -7,            // В хранилище недостаточно места для записи
    KVS_ERROR_STORAGE_FAILURE = -8,     // Неустранимая ошибка хранилища (повреждение данных, ошибка I/O)
    KVS_ERROR_UNKNOWN = -9              // Неизвестная/неспецифическая ошибка
} kvs_status;

#define KVS_KEY_SIZE 128             // Размер ключа


// Проверяет существование ключа в хранилище.
// key - указатель на ключ.
// Возвращает 1 если ключ существует, 0 если не найден, или отрицательное значение (код ошибки).
int kvs_exists(const void *key);

// Удаляет запись по ключу.
// key - ключ для удаления.
// Возвращает KVS_SUCCESS при успешном удалении, KVS_ERROR_KEY_NOT_FOUND если ключ не найден, или другой код ошибки.
kvs_status kvs_delete(const void *key);

// Получает значение по ключу.
// key       - ключ для поиска.
// value     - буфер для сохранения значения.
// value_len - на входе содержит размер буфера value, на выходе — фактический размер значения.
// Возвращает KVS_SUCCESS при успехе, KVS_ERROR_BUFFER_TOO_SMALL если буфер слишком мал, или другой код ошибки.
kvs_status kvs_get(const void *key, void *value, size_t *value_len);

// Сохраняет или обновляет значение по ключу.
// key       - ключ.
// key_len   - размер ключа.
// value     - указатель на данные значения.
// value_len - размер значения.
// Возвращает KVS_SUCCESS при успехе, KVS_ERROR_NO_SPACE если нет места, или другой код ошибки.
kvs_status kvs_put(const void *key, size_t key_len, const void *value, size_t value_len);

// Инициализирует KVS. Пытается загрузить существующее хранилище или создает новое.
// storage_size_bytes - размер пользовательской области данных.
// Возвращает KVS_SUCCESS при успехе или код ошибки.
kvs_status kvs_init(size_t storage_size_bytes);

// Деинициализирует KVS, освобождая все ресурсы.
void kvs_deinit(void);


#endif //SSDMMCSTORE_KVS_H
