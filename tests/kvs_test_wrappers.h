#ifndef SSDMMCSTORE_KVS_TEST_WRAPPERS_H
#define SSDMMCSTORE_KVS_TEST_WRAPPERS_H


#include "kvs.h"

// Обертка для kvs_init.
// Инициализирует хранилище и выводит результат операции.
void Kvs_init(size_t storage_size_bytes);

// Обертка для kvs_deinit.
// Деинициализирует хранилище.
void Kvs_deinit(void);

// Обертка для kvs_put.
// Сохраняет ключ-значение и выводит результат операции.
void Kvs_put(const void *key, size_t key_len, const void *value, size_t value_len);

// Обертка для kvs_get.
// Получает значение по ключу и выводит результат операции.
void Kvs_get(const void *key, void *value, size_t *value_len);

// Обертка для kvs_delete.
// Удаляет ключ и выводит результат операции.
void Kvs_delete(const void *key);

// Обертка для kvs_exists.
// Проверяет существование ключа и выводит результат.
void Kvs_exists(const void *key);

// Обертка для kvs_update
void Kvs_update(const void *key, const void *value, size_t value_len);


#endif