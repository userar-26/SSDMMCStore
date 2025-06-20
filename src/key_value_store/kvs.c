#include "kvs_internal.h"
#include "kvs_metadata.h"
#include "kvs_internal_io.h"
#include "kvs_valid.h"

int kvs_exists(const void *key)
{
    // Шаг 1: Проверяем базовые параметры
    if (!device) {
        return KVS_ERROR_NOT_INITIALIZED;
    }
    if (!key) {
        return KVS_ERROR_INVALID_PARAM;
    }
    if (device->key_count == 0) {
        return 0;
    }

    // Шаг 2: Ищем ключ в отсортированном key_index с помощью бинарного поиска
    uint32_t left = 0, right = device->key_count - 1;
    while (left <= right) {
        uint32_t mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);

        if (n == 0) {
            // Шаг 3: Ключ найден в key_index. Проверяем, валиден ли он на самом деле.
            // Если ключ помечен как невалидный, для пользователя его не существует.
            return is_key_valid(mid) == 1 ? 1 : 0;
        } else if (n < 0) {
            left = mid + 1;
        } else {
            // Защита от переполнения переменной
            if (mid == 0) {
                break;
            }
            right = mid - 1;
        }
    }
    return 0;
}

kvs_status kvs_delete(const void *key) {

    // Шаг 1: Проверяем базовые параметры
    if (!device) {
        return KVS_ERROR_NOT_INITIALIZED;
    }
    if (!key) {
        return KVS_ERROR_INVALID_PARAM;
    }
    if (device->key_count == 0) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 2: Ищем ключ в key_index
    uint32_t left = 0, right = device->key_count - 1, mid = 0;
    bool found = false;
    while (left <= right) {
        mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);
        if (n == 0) {
            found = true;
            break;
        } else if (n < 0) {
            left = mid + 1;
        } else {
            if (mid == 0) {
                break;
            }
            right = mid - 1;
        }
    }

    // Шаг 3: Проверяем, что ключ был найден и он валиден
    if (!found || is_key_valid(mid) != 1) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 4: Получаем информацию о расположении данных
    kvs_metadata temp_metadata;
    uint32_t metadata_offset = device->key_index[mid].metadata_offset;
    if (kvs_read_region(device->fp, metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 5: Физически очищаем области на диске
    uint32_t aligned_value_len = align_up(temp_metadata.value_size, device->superblock.word_size_bytes);
    if (kvs_clear_region(device->fp, temp_metadata.value_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (kvs_clear_region(device->fp, metadata_offset, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 6: Обновляем все служебные структуры в ОЗУ
    uint32_t slot_index = (metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
    if (kvs_update_single_metadata_crc(slot_index) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (bitmap_clear_metadata_slot(slot_index) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (crc_update_region(temp_metadata.value_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (rewrite_count_increment_region(metadata_offset, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (rewrite_count_increment_region(temp_metadata.value_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (bitmap_clear_region(temp_metadata.value_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 7: Удаляем ключ из key_index в ОЗУ
    for (uint32_t i = mid; i < device->key_count - 1; i++) {
        device->key_index[i] = device->key_index[i+1];
    }
    device->key_count--;

    // Шаг 8: Сохраняем все изменения служебных областей на диск
    if (kvs_persist_all_service_data() < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    return KVS_SUCCESS;
}

kvs_status kvs_get(const void *key, void *value, size_t *value_len)
{
    // Шаг 1: Проверяем базовые параметры
    if (!device) {
        return KVS_ERROR_NOT_INITIALIZED;
    }
    if (!key || !value || !value_len) {
        return KVS_ERROR_INVALID_PARAM;
    }
    if (device->key_count == 0) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 2: Ищем ключ в отсортированном key_index
    uint32_t left = 0, right = device->key_count - 1, mid = 0;
    bool found = false;
    while (left <= right) {
        mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);
        if (n == 0) {
            found = true;
            break;
        } else if (n < 0) {
            left = mid + 1;
        } else {
            if (mid == 0) {
                break;
            }
            right = mid - 1;
        }
    }

    // Шаг 3: Проверяем, что ключ был найден и валиден
    if (!found || is_key_valid(mid) != 1) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 4: Читаем метаданные ключа
    kvs_metadata temp_metadata;
    if (kvs_read_region(device->fp, device->key_index[mid].metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 5: Проверяем размер буфера пользователя
    if (*value_len < temp_metadata.value_size) {
        *value_len = temp_metadata.value_size;
        return KVS_ERROR_BUFFER_TOO_SMALL;
    }

    // Шаг 6: Читаем данные с диска во временный буфер, кратный размеру word_size
    uint32_t aligned_value_len = align_up(temp_metadata.value_size, device->superblock.word_size_bytes);
    uint8_t *temp_buffer = malloc(aligned_value_len);
    if (!temp_buffer) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (kvs_read_region(device->fp, temp_metadata.value_offset, temp_buffer, aligned_value_len) < 0) {
        free(temp_buffer);
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 7: Копируем данные в буфер пользователя и возвращаем результат
    memcpy(value, temp_buffer, temp_metadata.value_size);
    free(temp_buffer);
    *value_len = temp_metadata.value_size;

    return KVS_SUCCESS;
}

kvs_status kvs_put(const void *key, size_t key_len, const void *value, size_t value_len) {

    // Шаг 1: Проверяем базовые параметры
    if (!device) {
        return KVS_ERROR_NOT_INITIALIZED;
    }
    if (!key || !value || key_len != KVS_KEY_SIZE || value_len == 0) {
        return KVS_ERROR_INVALID_PARAM;
    }
    if (device->key_count >= device->superblock.max_key_count) {
        return KVS_ERROR_NO_SPACE;
    }

    // Шаг 2: Если ключ уже существует, удаляем его
    if (kvs_exists(key) == 1) {
        if (kvs_delete(key) != KVS_SUCCESS) {
            return KVS_ERROR_STORAGE_FAILURE;
        }
    }

    // Шаг 3: Выравниваем данные до размера word_size
    uint32_t aligned_value_len = align_up(value_len, device->superblock.word_size_bytes);
    uint8_t *padded_buffer = NULL;
    const void *final_value = value;
    if (aligned_value_len != value_len) {

        padded_buffer = malloc(aligned_value_len);

        if (!padded_buffer) {
            return KVS_ERROR_STORAGE_FAILURE;
        }

        memcpy(padded_buffer, value, value_len);
        memset(padded_buffer + value_len, 0xFF, aligned_value_len - value_len);
        final_value = padded_buffer;

    }

    // Шаг 4: Пытаемся найти место для метаданных. Если не находим, запускаем сборщик мусора.
    uint32_t metadata_offset = kvs_find_free_metadata_offset();
    if (metadata_offset == UINT32_MAX) {

        kvs_log("Нет места для метаданных, запускаем сборщик мусора...");

        // Проверяем освободил ли сборщик мусора данные
        if (kvs_gc() == 0) {
            if (padded_buffer)
                free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }

        // Повторная попытка после сборки мусора
        metadata_offset = kvs_find_free_metadata_offset();
        if (metadata_offset == UINT32_MAX) {
            if (padded_buffer)
                free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }

    }

    // Пытаемся найти место для данных. Если не находим, запускаем сборщик мусора.
    uint32_t data_offset = kvs_find_free_data_offset(aligned_value_len);
    if (data_offset == UINT32_MAX) {

        kvs_log("Нет места для данных, запускаем запускаем сборщик мусора...");

        if (kvs_gc() == 0) {
            if (padded_buffer)
                free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }

        // Повторная попытка после сборки мусора
        data_offset = kvs_find_free_data_offset(aligned_value_len);
        if (data_offset == UINT32_MAX) {
            if (padded_buffer)
                free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }

    }

    // Шаг 5: Записываем данные и метаданные на диск, изначально помечая данные, как невалидные в ОЗУ
    kvs_key_index_entry temp_key_entry;
    memcpy(temp_key_entry.key, key, KVS_KEY_SIZE);
    temp_key_entry.metadata_offset = metadata_offset;
    temp_key_entry.flags = 2;

    kvs_metadata temp_metadata;
    memcpy(temp_metadata.key, key, KVS_KEY_SIZE);
    temp_metadata.value_size = value_len;
    temp_metadata.value_offset = data_offset;

    device->key_index[device->key_count++] = temp_key_entry;

    if (kvs_write_region(device->fp, metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        device->key_count--;
        if (padded_buffer) free(padded_buffer);
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (kvs_write_region(device->fp, data_offset, final_value, aligned_value_len) < 0) {
        kvs_clear_region(device->fp, metadata_offset, sizeof(kvs_metadata));
        device->key_count--;
        if (padded_buffer) free(padded_buffer);
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (padded_buffer) {
        free(padded_buffer);
    }

    // Шаг 6: Обновляем служебные структуры в ОЗУ, и делаем быструю сортировку key_index
    qsort(device->key_index, device->key_count, sizeof(kvs_key_index_entry), kvs_key_index_entry_cmp);
    uint32_t slot_index = (metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
    for (uint32_t i = 0; i < device->key_count; i++) {
        if (device->key_index[i].metadata_offset == metadata_offset) {
            device->key_index[i].flags = 1;
            break;
        }
    }
    if (kvs_update_single_metadata_crc(slot_index) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (bitmap_set_metadata_slot(slot_index) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (crc_update_region(data_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (rewrite_count_increment_region(metadata_offset, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (rewrite_count_increment_region(data_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (bitmap_set_region(data_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 7: Сохраняем все изменения в служебных структурах на диск
    if (kvs_persist_all_service_data() < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    return KVS_SUCCESS;
}

