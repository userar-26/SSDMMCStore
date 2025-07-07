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
            // Шаг 3: Ключ найден в key_index. Проверяем его валидность
            return is_key_valid(mid) == 1 ? 1 : 0;
        } else if (n < 0) {
            left = mid + 1;
        } else {
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

    // Шаг 3: Проверяем, что ключ был найден и он полностью валиден
    if (!found || is_key_valid(mid) != 1) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 4: Получаем информацию о расположении данных
    kvs_metadata temp_metadata;
    uint32_t metadata_offset = device->key_index[mid].metadata_offset;
    if (kvs_read_region(device->fp, metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 5: Физически очищаем на диске область данных и область метаданных
    uint32_t aligned_value_len = align_up(temp_metadata.value_size, device->superblock.word_size_bytes);
    if (kvs_clear_region(device->fp, temp_metadata.value_offset, aligned_value_len) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (kvs_clear_region(device->fp, metadata_offset, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 6: Обновляем служебные структуры в ОЗУ
    uint32_t slot_index = (metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);

    if (bitmap_clear_metadata_slot(slot_index) < 0) {
        kvs_log("KVS_DELETE ВНИМАНИЕ: Не удалось сбросить бит в биткарте метаданных для слота %u", slot_index);
    }
    if (rewrite_count_increment_region(metadata_offset, sizeof(kvs_metadata)) < 0) {
        kvs_log("KVS_DELETE ВНИМАНИЕ: Не удалось увеличить счетчик перезаписи для метаданных");
    }
    if (rewrite_count_increment_region(temp_metadata.value_offset, aligned_value_len) < 0) {
        kvs_log("KVS_DELETE ВНИМАНИЕ: Не удалось увеличить счетчик перезаписи для данных");
    }
    if (bitmap_clear_region(temp_metadata.value_offset, aligned_value_len) < 0) {
        kvs_log("KVS_DELETE ВНИМАНИЕ: Не удалось сбросить биты в битовой карте данных");
    }

    // Шаг 7: Удаляем ключ из кеша key_index в ОЗУ
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

    // Шаг 3: Проверяем, что ключ был найден и он полностью валиден
    if (!found || is_key_valid(mid) != 1) {
        return KVS_ERROR_KEY_NOT_FOUND;
    }

    // Шаг 4: Читаем метаданные ключа
    kvs_metadata temp_metadata;
    if (kvs_read_region(device->fp, device->key_index[mid].metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 5: Проверяем, достаточно ли велик буфер пользователя
    if (*value_len < temp_metadata.value_size) {
        *value_len = temp_metadata.value_size;
        return KVS_ERROR_BUFFER_TOO_SMALL;
    }

    // Шаг 6: Читаем данные с диска
    uint32_t aligned_value_len = align_up(temp_metadata.value_size, device->superblock.word_size_bytes);
    uint8_t *temp_buffer = calloc(1,aligned_value_len);
    if (!temp_buffer) {
        return KVS_ERROR_STORAGE_FAILURE;
    }
    if (kvs_read_region(device->fp, temp_metadata.value_offset, temp_buffer, aligned_value_len) < 0) {
        free(temp_buffer);
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Шаг 7: Копируем точное количество байт в буфер пользователя
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

    // Шаг 2: Если ключ уже существует, возвращаем ошибку
    if (kvs_exists(key) == 1) {
        return KVS_ERROR_KEY_ALREADY_EXISTS;
    }

    // Шаг 3: Выравниваем данные до размера слова
    uint32_t aligned_value_len = align_up(value_len, device->superblock.word_size_bytes);
    uint8_t *padded_buffer = NULL;
    const void *final_value = value;
    if (aligned_value_len != value_len) {
        padded_buffer = calloc(1,aligned_value_len);
        if (!padded_buffer) {
            return KVS_ERROR_STORAGE_FAILURE;
        }
        memcpy(padded_buffer, value, value_len);
        memset(padded_buffer + value_len, 0xFF, aligned_value_len - value_len);
        final_value = padded_buffer;
    }

    // Шаг 4: Ищем место для метаданных. Если не находим, запускаем сборщик мусора.
    uint32_t metadata_offset = kvs_find_free_metadata_offset();
    while (metadata_offset == UINT32_MAX){
        kvs_log("Нет места для метаданных, запускаем сборщик мусора...");
        if(kvs_gc(CLEAN_METADATA) == 0){
            kvs_log("После очистки всего мусора, не нашлось места для метаданных");
            if (padded_buffer) free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }
        metadata_offset = kvs_find_free_metadata_offset();
    }

    // Шаг 5: Ищем место для данных. Если не находим, запускаем сборщик мусора.
    uint32_t data_offset = kvs_find_free_data_offset(aligned_value_len);
    while (data_offset == UINT32_MAX){
        kvs_log("Нет места для данных, запускаем сборщик мусора...");
        if( kvs_gc(CLEAN_DATA) == 0){
            kvs_log("После очистки всего мусора, не нашлось места для данных");
            if (padded_buffer) free(padded_buffer);
            return KVS_ERROR_NO_SPACE;
        }
        data_offset = kvs_find_free_data_offset(aligned_value_len);
    }

    // Шаг 6: Записываем данные и метаданные на диск
    kvs_key_index_entry temp_key_entry;
    memcpy(temp_key_entry.key, key, KVS_KEY_SIZE);
    temp_key_entry.metadata_offset = metadata_offset;
    temp_key_entry.flags = 2;

    kvs_metadata temp_metadata;
    memcpy(temp_metadata.key, key, KVS_KEY_SIZE);
    temp_metadata.value_size = value_len;
    temp_metadata.value_offset = data_offset;

    device->key_index[device->key_count++] = temp_key_entry;

    // Проверяем соответствует ли регион для записи биткарте данных
    // если нет, то очищаем те места, которые помечены в биткарте как пустые

    if (kvs_verify_and_prepare_region(data_offset,aligned_value_len) < 0) {
        device->key_count--;
        if (padded_buffer) free(padded_buffer);
        return KVS_ERROR_STORAGE_FAILURE;
    }

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

    // Шаг 7: Обновляем служебные структуры в ОЗУ
    qsort(device->key_index, device->key_count, sizeof(kvs_key_index_entry), kvs_key_index_entry_cmp);
    uint32_t slot_index = (metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);

    if (kvs_update_entry_crc(slot_index) < 0) {
        kvs_log("KVS_PUT ВНИМАНИЕ: Не удалось обновить единый CRC для слота %u", slot_index);
    }

    if (bitmap_set_metadata_slot(slot_index) < 0) {
        kvs_log("KVS_PUT ВНИМАНИЕ: Не удалось установить бит в биткарте метаданных для слота %u", slot_index);
    }
    if (rewrite_count_increment_region(metadata_offset, sizeof(kvs_metadata)) < 0) {
        kvs_log("KVS_PUT ВНИМАНИЕ: Не удалось увеличить счетчик перезаписи для метаданных");
    }
    if (rewrite_count_increment_region(data_offset, aligned_value_len) < 0) {
        kvs_log("KVS_PUT ВНИМАНИЕ: Не удалось увеличить счетчик перезаписи для данных");
    }
    if (bitmap_set_region(data_offset, aligned_value_len) < 0) {
        kvs_log("KVS_PUT ВНИМАНИЕ: Не удалось установить биты в битовой карте данных");
    }

    // Помечаем ключ как валидный в ОЗУ
    for (uint32_t i = 0; i < device->key_count; i++) {
        if (device->key_index[i].metadata_offset == metadata_offset) {
            device->key_index[i].flags = 1;
            break;
        }
    }

    // Шаг 8: Сохраняем все изменения в служебных структурах на диск
    if (kvs_persist_all_service_data() < 0) {
        return KVS_ERROR_STORAGE_FAILURE;
    }

    return KVS_SUCCESS;
}

kvs_status kvs_update(const void *key, const void *value, size_t value_len) {

    // Шаг 1: Проверка базовых параметров.
    if (!device) {
        return KVS_ERROR_NOT_INITIALIZED;
    }
    if (!key || !value || value_len == 0) {
        return KVS_ERROR_INVALID_PARAM;
    }

    // Шаг 2: Сначала удаляем старую запись.
    kvs_status delete_status = kvs_delete(key);

    if (delete_status != KVS_SUCCESS) {
        // Если удаление не удалось возвращаем ошибку.
        return delete_status;
    }

    // Шаг 3: После успешного удаления, создаем новую запись с тем же ключом.
    kvs_status put_status = kvs_put(key, KVS_KEY_SIZE, value, value_len);

    if (put_status != KVS_SUCCESS) {
        // Критическая ошибка: старые данные удалены, новые не записаны.
        kvs_log("КРИТИЧЕСКАЯ ОШИБКА UPDATE: ключ '%s' был удален, но не смог быть записан заново. Код ошибки: %d", (char*)key, put_status);
        return KVS_ERROR_STORAGE_FAILURE;
    }

    return KVS_SUCCESS;
}