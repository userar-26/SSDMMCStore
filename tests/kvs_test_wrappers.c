#include "kvs.h"
#include "kvs_test_wrappers.h"

// Вспомогательная функция для преобразования кода ошибки kvs_status в строку
const char* get_kvs_error_string(kvs_status status) {
    switch (status) {
        case KVS_SUCCESS:
            return "Операция успешна";
        case KVS_ERROR_NOT_INITIALIZED:
            return "Хранилище не инициализировано";
        case KVS_ERROR_ALREADY_INITIALIZED:
            return "Хранилище уже было инициализировано";
        case KVS_ERROR_INVALID_PARAM:
            return "Передан неверный параметр";
        case KVS_ERROR_KEY_NOT_FOUND:
            return "Ключ не найден";
        case KVS_ERROR_KEY_ALREADY_EXISTS:
            return "Ключ уже существует";
        case KVS_ERROR_BUFFER_TOO_SMALL:
            return "Буфер для чтения слишком мал";
        case KVS_ERROR_NO_SPACE:
            return "Недостаточно места";
        case KVS_ERROR_STORAGE_FAILURE:
            return "Критическая ошибка хранилища";
        default:
            return "Неизвестная ошибка";
    }
}

void Kvs_init(size_t storage_size_bytes) {
    kvs_status status = kvs_init(storage_size_bytes);
    if (status == KVS_SUCCESS) {
        printf("KVS_INIT: Хранилище размером %zu байт успешно инициализировано.\n", storage_size_bytes);
    } else {
        printf("KVS_INIT: Ошибка инициализации. Причина: %s.\n", get_kvs_error_string(status));
    }
}

void Kvs_deinit(void) {
    kvs_deinit();
    printf("KVS_DEINIT: Хранилище успешно деинициализировано.\n");
}

void Kvs_put(const void *key, size_t key_len, const void *value, size_t value_len) {
    kvs_status status = kvs_put(key, key_len, value, value_len);
    if (status == KVS_SUCCESS) {
        printf("KVS_PUT: Ключ '%.*s' (размер: %zu) успешно добавлен.\n", (int)strnlen(key, key_len), (const char*)key, value_len);
    } else {
        printf("KVS_PUT: Ошибка добавления ключа '%.*s'. Причина: %s.\n", (int)strnlen(key, key_len), (const char*)key, get_kvs_error_string(status));
    }
}

void Kvs_get(const void *key, void *value, size_t *value_len) {
    size_t initial_size = *value_len;
    kvs_status status = kvs_get(key, value, value_len);
    if (status == KVS_SUCCESS) {
        printf("KVS_GET: Ключ '%.*s' успешно прочитан (размер: %zu).\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key, *value_len);
    } else if (status == KVS_ERROR_BUFFER_TOO_SMALL) {
        printf("KVS_GET: Ошибка чтения ключа '%.*s'. Причина: Буфер слишком мал (требуется %zu, предоставлено %zu).\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key, *value_len, initial_size);
    } else {
        printf("KVS_GET: Ошибка чтения ключа '%.*s'. Причина: %s.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key, get_kvs_error_string(status));
    }
}

void Kvs_delete(const void *key) {
    kvs_status status = kvs_delete(key);
    if (status == KVS_SUCCESS) {
        printf("KVS_DELETE: Ключ '%.*s' успешно удален.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key);
    } else {
        printf("KVS_DELETE: Ошибка удаления ключа '%.*s'. Причина: %s.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key, get_kvs_error_string(status));
    }
}

void Kvs_exists(const void *key) {
    int result = kvs_exists(key);
    if (result == 1) {
        printf("KVS_EXISTS: Ключ '%.*s' найден в хранилище.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key);
    } else if (result == 0) {
        printf("KVS_EXISTS: Ключ '%.*s' не найден в хранилище.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key);
    } else {
        printf("KVS_EXISTS: Ошибка проверки ключа '%.*s'. Причина: %s.\n", (int)strnlen(key, KVS_KEY_SIZE), (const char*)key, get_kvs_error_string((kvs_status)result));
    }
}