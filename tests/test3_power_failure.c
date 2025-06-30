#include "kvs_test_wrappers.h"
#include "../src/ssdmmc_sim/ssdmmc_sim.h" // Нужен для вызова функции сбоя
#include <stdio.h>
#include <string.h>

// --- Основной тестовый сценарий ---
int main(int argc, char *argv[]) {

    const char* key_A = "key_before_crash";
    const char* data_A = "this_data_must_survive";

    const char* key_B = "key_during_crash";
    const char* data_B = "this_data_should_not_exist_or_be_corrupt";

    // РЕЖИМ 1: Подготовка
    if (argc > 1 && strcmp(argv[1], "prepare") == 0) {
        printf("--- Фаза 1: Подготовка. Создаем хранилище и записываем первый ключ. ---\n");
        remove("../data/kvs_storage.bin");
        Kvs_init(1024 * 128);
        Kvs_put(key_A, KVS_KEY_SIZE, data_A, strlen(data_A) + 1);
        Kvs_deinit();
        printf("--- Подготовка завершена. ---\n");
        return 0;
    }

    // РЕЖИМ 2: Вызов сбоя
    if (argc > 1 && strcmp(argv[1], "crash") == 0) {
        printf("\n--- Фаза 2: Вызов сбоя. Пытаемся записать второй ключ. ---\n");
        Kvs_init(1024 * 128);

        // Устанавливаем "таймер": программа аварийно завершится после 10-й операции записи слова
        ssdmmc_sim_set_write_failure_countdown(10);

        // Эта операция не завершится
        Kvs_put(key_B, KVS_KEY_SIZE, data_B, strlen(data_B) + 1);

        Kvs_deinit(); // До сюда выполнение не дойдет
        return 0;
    }

    // РЕЖИМ 3: Проверка восстановления
    if (argc > 1 && strcmp(argv[1], "verify") == 0) {
        printf("\n--- Фаза 3: Проверка восстановления после сбоя. ---\n");
        Kvs_init(1024 * 128);

        printf("  Проверка состояния:\n");
        Kvs_exists(key_A); // Этот ключ ОБЯЗАН существовать и быть валидным

        char buffer[100];
        size_t size = sizeof(buffer);
        printf("      -> "); Kvs_get(key_A, buffer, &size);

        Kvs_exists(key_B); // Этот ключ либо не должен существовать, либо быть невалидным

        Kvs_deinit();
        printf("--- Проверка завершена. ---\n");
        return 0;
    }

    // Если запустить без аргументов, показываем инструкцию
    printf("Пожалуйста, запустите тест последовательно с аргументами:\n");
    printf("1. %s prepare\n", argv[0]);
    printf("2. %s crash\n", argv[0]);
    printf("3. %s verify\n", argv[0]);

    return 0;
}