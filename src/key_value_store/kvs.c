#include "kvs_internal.h"
#include "kvs_metadata.h"
#include "kvs_internal_io.h"

int kvs_exists(const void *key)
{
    if (!device)
        return -1;

    if (!key)
        return -2;

    if (device->key_count == 0)
        return -3;

    // Первый бинарный поиск
    uint32_t left = 0, right = device->key_count - 1;
    while (left <= right) {
        uint32_t mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);
        if (n == 0) {
            if (is_key_valid(mid))
                return 1;
            // Ключ найден, но невалиден — очищаем невалидные данные и пересобираем индекс

            if(kvs_gc() == 0) {
                kvs_log("Критичная ошибка: после проведения очистки невалидных данных, таковы не были удалены");
                kvs_free_device();
                exit(1);
            }

                // Повторный бинарный поиск после очистки
            left = 0;
            right = device->key_count - 1;
            while (left <= right) {
                uint32_t mid2 = left + (right - left) / 2;
                int n2 = memcmp(device->key_index[mid2].key, key, KVS_KEY_SIZE);
                if (n2 == 0) {
                    kvs_log("Критичная ошибка: после очистки невалидных данных, ключ, который считался невалидным, не был удален (key: %.*s)", KVS_KEY_SIZE, key);
                    kvs_free_device();
                    exit(1);
                } else if (n2 < 0) {
                    left = mid2 + 1;
                } else {
                    right = mid2 - 1;
                }
            }
            // Ключ был невалиден и удален
            return 0;
        } else if (n < 0) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return 0;
}


int kvs_delete(const void *key)
{
    if (!device)
        return -1;

    if (!key)
        return -2;

    if (device->key_count == 0)
        return -4;

    uint32_t left = 0, right = device->key_count - 1, mid = 0;
    while (left <= right) {
        mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);
        if (n == 0)
            break;
        else if (n < 0)
            left = mid + 1;
        else
            right = mid - 1;
    }
    if (left > right)
        return -6;

    if (!is_key_valid(mid)){

        if(kvs_gc() == 0){
            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, таковы не были удалены");
            kvs_free_device();
            exit(1);
        }

        if(kvs_exists(key)){

            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, ключ %.*s не был удален",KVS_KEY_SIZE,key);
            kvs_free_device();
            exit(1);
        }

        return 0;
    }
    else{
        kvs_metadata temp;
        uint32_t temp_metadata_offset = device->key_index[mid].metadata_offset;

        if (kvs_read_region(device->fp, temp_metadata_offset, &temp, sizeof(kvs_metadata)) < 0) {
            kvs_log("Ошибка: не удалось считать блок метаданных (metadata_offset = %u)", temp_metadata_offset);
            return -8;
        }
        if (kvs_clear_region(device->fp,temp.value_offset,temp.value_size) < 0){
            kvs_log("Ошибка: не удалось удалить блок данных для метаданных (metadata_offset = %u)", temp_metadata_offset);
            return -9;
        }
        if (kvs_clear_region(device->fp,temp_metadata_offset,sizeof(kvs_metadata)) < 0){
            kvs_log("Ошибка: не удалось удалить блок данных для метаданных (metadata_offset = %u)", temp_metadata_offset);
            return -10;
        }

        // Обернуть
        crc_update_region(temp.value_offset, temp.value_size);
        rewrite_count_increment_region(temp_metadata_offset, sizeof(kvs_metadata));
        rewrite_count_increment_region(temp.value_offset, temp.value_size);
        bitmap_clear_region(temp_metadata_offset, sizeof(kvs_metadata));
        bitmap_clear_region(temp.value_offset, temp.value_size);

        return 0;
    }
}


int kvs_get(const void *key, void *value, size_t *value_len)
{
    if (!device)
        return -1;

    if (!key)
        return -2;

    if (!value || !value_len)
        return -4;

    uint32_t left = 0, right = device->key_count - 1, mid = 0;
    while (left <= right) {
        mid = left + (right - left) / 2;
        int n = memcmp(device->key_index[mid].key, key, KVS_KEY_SIZE);
        if (n == 0)
            break;
        else if (n < 0)
            left = mid + 1;
        else
            right = mid - 1;
    }
    if (left > right)
        return -6;

    if(is_key_valid(mid)){

        kvs_metadata temp;
        uint32_t temp_metadata_offset = device->key_index[mid].metadata_offset;
        if (kvs_read_region(device->fp, temp_metadata_offset, &temp, sizeof(kvs_metadata)) < 0) {
            kvs_log("Ошибка: не удалось считать блок метаданных (metadata_offset = %u)", temp_metadata_offset);
            return -7;
        }
        if (kvs_read_region(device->fp, temp.value_offset, value, temp.value_size) < 0) {
            kvs_log("Ошибка: не удалось считать блок данных (data_offset = %u)", temp.value_offset);
            return -9;
        }
        *value_len = temp.value_size;
        return 0;
    }
    else{
        if(kvs_gc() == 0){
            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, таковы не были удалены");
            kvs_free_device();
            exit(1);
        }

        if(kvs_exists(key)){
            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, ключ %.*s не был удален",KVS_KEY_SIZE,key);
            kvs_free_device();
            exit(1);
        }

        return -2;
    }
}


int kvs_put(const void *key, size_t key_len, const void *value, size_t value_len)
{
    if (!device)
        return -1;
    if (!key || key_len == 0)
        return -2;
    if (key_len != KVS_KEY_SIZE)
        return -3;
    if (!value || value_len == 0)
        return -4;
    if (device->key_count >= device->superblock.max_key_count)
        return -5;

    // Если ключ уже существует, удаляем старое значение
    int exists = kvs_exists(key);
    if (exists == 1) {
        if (kvs_delete(key) < 0)
            return -6;
    } else if (exists < 0) {
        return -7;
    }

    // Поиск свободных регионов
    uint32_t metadata_offset = kvs_find_free_metadata_offset();
    if (metadata_offset == INT32_MAX)
        return -8;
    uint32_t data_offset     = kvs_find_free_data_offset(value_len);
    if (data_offset == INT32_MAX)
        return -9;

    // 1. Создаем key_index_entry с флагом "невалидный" (2)
    kvs_key_index_entry temp_key_entry;
    memset(&temp_key_entry, 0, sizeof(temp_key_entry));
    memcpy(temp_key_entry.key, key, KVS_KEY_SIZE);
    temp_key_entry.metadata_offset = metadata_offset;
    temp_key_entry.flags = 2; // невалидный

    // 2. Заполняем метаданные
    kvs_metadata temp_metadata;
    memset(&temp_metadata, 0, sizeof(temp_metadata));
    memcpy(temp_metadata.key, key, KVS_KEY_SIZE);
    temp_metadata.value_size = value_len;
    temp_metadata.value_offset = data_offset;

    // 3. Добавляем в индекс (только в память, на диск не пишем)
    if (device->key_count >= device->superblock.max_key_count)
        return -12;
    device->key_index[device->key_count++] = temp_key_entry;

    // Если регион не пустой, хотя битовая карта, сказала, что пуст, то делает сборку мусора
    if (!is_data_region_empty( metadata_offset, sizeof(kvs_metadata))){
        if(kvs_gc() == 0){
            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, таковы не были удалены");
            kvs_free_device();
            exit(1);
        }
    }

    // 3. Записываем метаданные
    if (kvs_write_region(device->fp, metadata_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        kvs_log("Ошибка: не удалось записать метаданные для ключа: %.*s", KVS_KEY_SIZE, key);
        device->key_count--;
        return -14;
    }

    // Если регион не пустой, хотя битовая карта, сказала, что пуст, то делает сборку мусора
    if (!is_data_region_empty( data_offset, value_len)){
        if(kvs_gc() == 0){
            kvs_log("Критичная ошибка: после проведения очистки невалидных данных, таковы не были удалены");
            kvs_free_device();
            exit(1);
        }
    }

    // 4. Записываем данные
    if (kvs_write_region(device->fp, data_offset, value, value_len) < 0) {
        kvs_log("Ошибка: не удалось записать данные для ключа: %.*s", KVS_KEY_SIZE, key);
        kvs_clear_region(device->fp, data_offset, value_len);
        device->key_count--;
        return -13;
    }

    // 5. Теперь делаем ключ валидным
    device->key_index[device->key_count - 1].flags = 1;

    // 6. Делаем быструю сортировку массива key_index
    qsort(device->key_index,device->key_count,sizeof(*device->key_index),kvs_key_index_entry_cmp);

    // Служебные обновления
    // Обернуть
    crc_update_region(temp_metadata.value_offset, temp_metadata.value_size);
    rewrite_count_increment_region(metadata_offset, sizeof(kvs_metadata));
    rewrite_count_increment_region(data_offset, value_len);
    bitmap_set_region(metadata_offset, sizeof(kvs_metadata));
    bitmap_set_region(data_offset, value_len);

    return 0;
}

