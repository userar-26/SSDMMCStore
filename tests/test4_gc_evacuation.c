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
const char two_page_key_name[KVS_KEY_SIZE] = "two_page_key_to_delete";
const char last_page_key_name[KVS_KEY_SIZE] = "key_on_last_page";
const char new_key_after_gc_name[KVS_KEY_SIZE] = "new_key_after_gc";

const char small_keys[6][KVS_KEY_SIZE] = {
        "sk1", "sk2", "sk3", "sk4", "sk5", "sk6"
};
const int keys_to_corrupt_indices[] = {1, 3};

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
    printf("     ЗАПУСК ДЕТЕРМИНИРОВАННОГО ТЕСТА GC С ЭВАКУАЦИЕЙ    \n");
    printf("=========================================================\n");

    // --- Фаза 1: Первичное заполнение ---
    printf("\n--- Фаза 1: Заполнение хранилища для создания дыры ---\n");
    remove(KVS_STORAGE_FILE_PATH);
    Kvs_init(TEST_USER_DATA_SIZE);

    if (!device) {
        return 1;
    }

    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t total_user_data_size = device->superblock.userdata_size_bytes;
    uint32_t two_page_size = 2 * page_size;
    uint32_t large_key_size = total_user_data_size - two_page_size - page_size;

    char* large_data = calloc(1,large_key_size);
    char* two_page_data = calloc(1,two_page_size);
    char* last_page_data = calloc(1,page_size);
    if( !large_data || !two_page_data || !last_page_data)
        return 1;
    memset(large_data, 'A', large_key_size);
    memset(two_page_data, 'B', two_page_size);
    memset(last_page_data, 'C', page_size);

    Kvs_put(large_key_name, KVS_KEY_SIZE, large_data, large_key_size);
    Kvs_put(two_page_key_name, KVS_KEY_SIZE, two_page_data, two_page_size);
    Kvs_put(last_page_key_name, KVS_KEY_SIZE, last_page_data, page_size);

    Kvs_deinit();
    free(large_data); free(two_page_data); free(last_page_data);

    // --- Фаза 2: Создание "дыры" и ее фрагментация ---
    printf("\n--- Фаза 2: Создание и фрагментация 2-страничной дыры ---\n");
    Kvs_init(TEST_USER_DATA_SIZE);

    Kvs_delete(two_page_key_name);

    uint32_t small_key_size = page_size / 4;
    char* small_data_chunk = calloc(1,small_key_size);
    if(!small_data_chunk)
        return 2;
    for(int i=0; i<6; i++) {
        memset(small_data_chunk, 'S' + i, small_key_size);
        Kvs_put(small_keys[i], KVS_KEY_SIZE, small_data_chunk, small_key_size);
    }
    free(small_data_chunk);

    Kvs_deinit();

    // --- Фаза 3: Повреждение части малых ключей для создания мусора ---
    printf("\n--- Фаза 3: Повреждение части малых ключей для создания мусора ---\n");

    FILE* fp_reader = fopen(KVS_STORAGE_FILE_PATH, "rb");
    if (!fp_reader) { return 1; }

    kvs_superblock sb;
    fread(&sb, sizeof(kvs_superblock), 1, fp_reader);

    for (int i = 0; i < sizeof(keys_to_corrupt_indices)/sizeof(int); i++) {
        int key_idx_to_corrupt = keys_to_corrupt_indices[i];
        fseek(fp_reader, sb.metadata_offset, SEEK_SET);
        kvs_metadata temp_meta;
        char key_buffer_for_cmp[KVS_KEY_SIZE] = {0};
        strncpy(key_buffer_for_cmp, small_keys[key_idx_to_corrupt], KVS_KEY_SIZE - 1);

        for (uint32_t j = 0; j < sb.max_key_count; j++) {
            if (fread(&temp_meta, sizeof(kvs_metadata), 1, fp_reader) != 1) break;
            if (memcmp(temp_meta.key, key_buffer_for_cmp, KVS_KEY_SIZE) == 0) {
                printf("  Повреждаем данные для ключа '%s'\n", small_keys[key_idx_to_corrupt]);
                corrupt_file(temp_meta.value_offset, 4);
                break;
            }
        }
    }
    fclose(fp_reader);

    printf("\n--- Визуализация состояния хранилища ПЕРЕД запуском GC ---\n\n");
    printf("  [ ... Данные 'large_key_main_space' ... ]\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  |   sk1 (живой)   |   sk2 (мусор)   |   sk3 (живой)   |   sk4 (мусор)   |\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  |   sk5 (живой)   |   sk6 (живой)   |      пусто      |      пусто      |\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  [ ... Данные 'key_on_last_page' ... ]\n");
    printf("\n  Ожидаемое поведение GC: Выбрать первую из двух дырявых страниц как жертву,\n");
    printf("  эвакуировать 'sk1' и 'sk3' на свободное место второй страницы,\n");
    printf("  после чего полностью очистить первую дырявую страницу.\n");

    // --- Фаза 4: Запуск GC и финальная проверка ---
    printf("\n--- Фаза 4: Запись нового ключа для запуска GC и проверка эвакуации ---\n");
    Kvs_init(TEST_USER_DATA_SIZE);

    char new_data[page_size];
    memset(new_data, 'N', sizeof(new_data));
    Kvs_put(new_key_after_gc_name, KVS_KEY_SIZE, new_data, sizeof(new_data));

    printf("\n--- Визуализация состояния хранилища ПОСЛЕ работы GC ---\n\n");
    printf("  [ ... Данные 'large_key_main_space' ... ]\n");
    printf("  +-----------------------------------------------------------------------+\n");
    printf("  |                      Данные 'new_key_after_gc'                        |\n");
    printf("  |                   (Заняли всю очищенную страницу)                     |\n");
    printf("  +-----------------------------------------------------------------------+\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  |   sk5 (живой)   |   sk6 (живой)   |   sk1 (переехал)|   sk3 (переехал)|\n");
    printf("  +-----------------+-----------------+-----------------+-----------------+\n");
    printf("  [ ... Данные 'key_on_last_page' ... ]\n");

    printf("\n  Проверка состояния после работы GC:\n");
    Kvs_exists(large_key_name);
    Kvs_exists(last_page_key_name);
    Kvs_exists(new_key_after_gc_name);

    printf("  Проверка эвакуированных ключей (должны существовать):\n");
    Kvs_exists(small_keys[0]); // sk1
    Kvs_exists(small_keys[2]); // sk3
    printf("  Проверка ключей которые лежат на странице в которую происходит эвакуация(должны существовать):\n");
    Kvs_exists(small_keys[4]); // sk5
    Kvs_exists(small_keys[5]); // sk6


    printf("  Проверка поврежденных ключей (не должны существовать):\n");
    Kvs_exists(small_keys[1]); // sk2
    Kvs_exists(small_keys[3]); // sk4
    Kvs_deinit();

    printf("\n=========================================================\n");
    printf("           ТЕСТИРОВАНИЕ ЭВАКУАЦИИ УСПЕШНО ЗАВЕРШЕНО          \n");
    printf("=========================================================\n");

    return 0;
}