#include "kvs_test_wrappers.h"
#include "../src/key_value_store/kvs_internal.h"
#include "kvs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

// --- Конфигурация теста ---
#define TEST_USER_DATA_SIZE (1024 * 512)
const char* KVS_STORAGE_FILE_PATH = "../data/kvs_storage.bin";

// --- Тестовые данные ---
const char large_key_name[KVS_KEY_SIZE] = "large_key_main_space";
const char key_to_corrupt[KVS_KEY_SIZE] = "key_to_corrupt";
const char key_on_last_page[KVS_KEY_SIZE] = "key_on_last_page";
const char new_key_name[KVS_KEY_SIZE]   = "new_key_after_gc";

// --- Вспомогательные функции ---

bool corrupt_file(long offset, size_t size) {
    FILE* fp = fopen(KVS_STORAGE_FILE_PATH, "r+b");
    if (!fp) {
        printf("  ПРОВЕРКА: КРИТИЧЕСКАЯ ОШИБКА! Не удалось открыть файл %s для повреждения.\n", KVS_STORAGE_FILE_PATH);
        return false;
    }
    fseek(fp, offset, SEEK_SET);
    void* garbage = calloc(1,size);
    memset(garbage, 5, size);
    size_t written = fwrite(garbage, 1, size, fp);
    free(garbage);
    fclose(fp);
    if (written != size) {
        printf("  ПРОВЕРКА: КРИТИЧЕСКАЯ ОШИБКА! Не удалось записать поврежденные данные в файл.\n");
        return false;
    }
    return true;
}

// --- Основной тестовый сценарий ---

int main() {
    printf("=========================================================\n");
    printf("          ЗАПУСК ТЕСТА СБОРЩИКА МУСОРА (GC)           \n");
    printf("=========================================================\n");

    // --- Фаза 1: Подготовка хранилища ---
    printf("\n--- Фаза 1: Заполнение хранилища точно под завязку ---\n");
    remove(KVS_STORAGE_FILE_PATH);
    Kvs_init(TEST_USER_DATA_SIZE);

    if (!device) {
        printf("Критическая ошибка: не удалось инициализировать хранилище. Тест прерван.\n");
        return 1;
    }

    // Рассчитываем размеры ключей
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t total_user_data_size = device->superblock.userdata_size_bytes;
    uint32_t large_key_size = total_user_data_size - (2 * page_size);

    printf("  Информация: Размер большого ключа: %u, размер малых ключей: %u\n", large_key_size, page_size);

    // Выделяем память и заполняем данные
    char* data1 = calloc(1,large_key_size);
    char* data2 = calloc(1,page_size);
    char* data3 = calloc(1,page_size);
    memset(data1, 'A', large_key_size);
    memset(data2, 'B', page_size); // Эту область будем повреждать
    memset(data3, 'C', page_size);

    // Записываем ключи
    Kvs_put(large_key_name, KVS_KEY_SIZE, data1, large_key_size);
    Kvs_put(key_to_corrupt, KVS_KEY_SIZE, data2, page_size);
    Kvs_put(key_on_last_page, KVS_KEY_SIZE, data3, page_size);

    Kvs_deinit();
    free(data1); free(data2); free(data3);

    // --- Фаза 2: Повреждение данных второго ключа ---
    printf("\n--- Фаза 2: Повреждение данных второго ключа для создания мусора ---\n");

    long data_to_corrupt_offset = 0;
    FILE* fp_reader = fopen(KVS_STORAGE_FILE_PATH, "rb");
    if (!fp_reader) {
        printf("Критическая ошибка: не удалось открыть файл для чтения. Тест прерван.\n");
        return 1;
    }

    kvs_superblock sb;
    fread(&sb, sizeof(kvs_superblock), 1, fp_reader);
    fseek(fp_reader, sb.metadata_offset, SEEK_SET);

    kvs_metadata temp_meta;
    char key_buffer_for_cmp[KVS_KEY_SIZE] = {0};
    strncpy(key_buffer_for_cmp, key_to_corrupt, KVS_KEY_SIZE - 1);

    for (uint32_t i = 0; i < sb.max_key_count; i++) {
        if (fread(&temp_meta, sizeof(kvs_metadata), 1, fp_reader) != 1) break;
        if (memcmp(temp_meta.key, key_buffer_for_cmp, KVS_KEY_SIZE) == 0) {
            data_to_corrupt_offset = temp_meta.value_offset;
            break;
        }
    }
    fclose(fp_reader);

    if (data_to_corrupt_offset == 0) {
        printf("  ПРОВЕРКА: Не удалось найти смещение данных для повреждения. Тест пропущен.\n");
        return 1;
    }

    printf("  Целенаправленное повреждение данных ключа '%s' по смещению %ld.\n", key_to_corrupt, data_to_corrupt_offset);
    corrupt_file(data_to_corrupt_offset, 16);

    // --- Фаза 3: Запуск GC и проверка результата ---
    printf("\n--- Фаза 3: Запись нового ключа для запуска GC и проверка ---\n");
    Kvs_init(TEST_USER_DATA_SIZE);

    printf("  Проверка состояния перед запуском GC:\n");
    Kvs_exists(large_key_name); // Этот ключ должен быть на месте
    Kvs_exists(key_to_corrupt); // А этот ключ должен быть не найден из-за повреждения
    Kvs_exists(key_on_last_page); // Этот ключ тоже должен быть на месте

    printf("\n  Запись нового ключа, которая должна запустить GC...\n");
    char new_data[100];
    memset(new_data, 'N', sizeof(new_data));
    Kvs_put(new_key_name, KVS_KEY_SIZE, new_data, sizeof(new_data));

    printf("\n  Проверка состояния после работы GC:\n");
    Kvs_exists(large_key_name);
    Kvs_exists(key_on_last_page);
    Kvs_exists(new_key_name);
    Kvs_exists(key_to_corrupt);

    Kvs_deinit();

    printf("\n=========================================================\n");
    printf("             ТЕСТИРОВАНИЕ GC ЗАВЕРШЕНО             \n");
    printf("=========================================================\n");

    return 0;
}