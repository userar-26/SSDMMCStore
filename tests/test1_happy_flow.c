#include "kvs.h"
#include "kvs_test_wrappers.h"

// --- Конфигурация теста ---
#define STORAGE_SIZE (1024 * 1024)

// Количество ключей в каждой категории будет случайным, в диапазоне [MIN_KEYS..MAX_KEYS]
#define MIN_KEYS 10
#define MAX_KEYS 20

// Диапазоны размеров для данных
#define SMALL_DATA_MIN  10
#define SMALL_DATA_MAX  200
#define MEDIUM_DATA_MIN 1000
#define MEDIUM_DATA_MAX 4000
#define LARGE_DATA_MIN  10000
#define LARGE_DATA_MAX  20000

// Структура для хранения информации о каждом тестовом наборе данных
typedef struct {
    char key[KVS_KEY_SIZE];
    void *data;
    size_t size;
} TestData;

int main() {

    srand(time(NULL));

    printf("====================================================\n");
    printf("     ЗАПУСК ТЕСТИРОВАНИЯ    \n");
    printf("====================================================\n\n");

    Kvs_init(STORAGE_SIZE);

    int num_small_keys  = MIN_KEYS + rand() % (MAX_KEYS - MIN_KEYS + 1);
    int num_medium_keys = MIN_KEYS + rand() % (MAX_KEYS - MIN_KEYS + 1);
    int num_large_keys  = MIN_KEYS + rand() % (MAX_KEYS - MIN_KEYS + 1);
    int total_keys      = num_small_keys + num_medium_keys + num_large_keys;

    // Создаем массив для хранения всей нашей тестовой информации
    TestData *all_tests = calloc(1,total_keys * sizeof(TestData));
    if(!all_tests)
        return 1;
    int current_key_index = 0;

    printf("--- Фаза 1: Генерация и запись тестовых данных ---\n");
    printf("Будет создано: %d маленьких, %d средних, %d больших ключей. Всего: %d\n\n",
           num_small_keys, num_medium_keys, num_large_keys, total_keys);

    // 1.1: Генерация маленьких данных
    for (int i = 0; i < num_small_keys; i++) {
        sprintf(all_tests[current_key_index].key, "small_key_%d", i);
        all_tests[current_key_index].size = SMALL_DATA_MIN + rand() % (SMALL_DATA_MAX - SMALL_DATA_MIN + 1);
        all_tests[current_key_index].data = calloc(1,all_tests[current_key_index].size);
        memset(all_tests[current_key_index].data, 'A' + (i % 26), all_tests[current_key_index].size);

        Kvs_put(all_tests[current_key_index].key, KVS_KEY_SIZE, all_tests[current_key_index].data, all_tests[current_key_index].size);
        current_key_index++;
    }

    // 1.2: Генерация средних данных
    for (int i = 0; i < num_medium_keys; i++) {
        sprintf(all_tests[current_key_index].key, "medium_key_%d", i);
        all_tests[current_key_index].size = MEDIUM_DATA_MIN + rand() % (MEDIUM_DATA_MAX - MEDIUM_DATA_MIN + 1);
        all_tests[current_key_index].data = calloc(1,all_tests[current_key_index].size);
        memset(all_tests[current_key_index].data, 'a' + (i % 26), all_tests[current_key_index].size);

        Kvs_put(all_tests[current_key_index].key, KVS_KEY_SIZE, all_tests[current_key_index].data, all_tests[current_key_index].size);
        current_key_index++;
    }

    // 1.3: Генерация больших данных
    for (int i = 0; i < num_large_keys; i++) {
        sprintf(all_tests[current_key_index].key, "large_key_%d", i);
        all_tests[current_key_index].size = LARGE_DATA_MIN + rand() % (LARGE_DATA_MAX - LARGE_DATA_MIN + 1);
        all_tests[current_key_index].data = calloc(1,all_tests[current_key_index].size);
        memset(all_tests[current_key_index].data, '0' + (i % 10), all_tests[current_key_index].size);

        Kvs_put(all_tests[current_key_index].key, KVS_KEY_SIZE, all_tests[current_key_index].data, all_tests[current_key_index].size);
        current_key_index++;
    }

    // --- Фаза 2: Проверка существования всех ключей ---
    printf("\n--- Фаза 2: Проверка существования всех %d ключей ---\n", total_keys);
    for (int i = 0; i < total_keys; i++) {
        Kvs_exists(all_tests[i].key);
    }

    // --- Фаза 3: Перезапуск и проверка целостности данных ---
    printf("\n--- Фаза 3: Перезапуск хранилища и проверка целостности данных ---\n");
    Kvs_deinit();
    Kvs_init(STORAGE_SIZE);

    for (int i = 0; i < total_keys; i++) {
        size_t buffer_size = all_tests[i].size;
        void *read_buffer = calloc(1,buffer_size);
        if(!read_buffer){
            free(all_tests);
            return 2;
        }

        Kvs_get(all_tests[i].key, read_buffer, &buffer_size);

        // Сравниваем прочитанные данные с оригиналом
        if (buffer_size == all_tests[i].size && memcmp(read_buffer, all_tests[i].data, buffer_size) == 0) {
            printf("  ПРОВЕРКА: Данные для ключа '%s' корректны.\n", all_tests[i].key);
        } else {
            printf("  ПРОВЕРКА: ОШИБКА! Данные для ключа '%s' повреждены или имеют неверный размер.\n", all_tests[i].key);
        }
        free(read_buffer);
    }

    // --- Фаза 4: Удаление всех ключей и финальная проверка ---
    printf("\n--- Фаза 4: Удаление всех %d ключей и финальная проверка ---\n", total_keys);
    for (int i = 0; i < total_keys; i++) {
        Kvs_delete(all_tests[i].key);
        Kvs_exists(all_tests[i].key);
    }

    Kvs_deinit();

    for (int i = 0; i < total_keys; i++) {
        free(all_tests[i].data);
    }
    free(all_tests);

    printf("\n====================================================\n");
    printf("             ТЕСТ ЗАВЕРШЕН             \n");
    printf("====================================================\n");

    return 0;
}