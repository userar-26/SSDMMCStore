#include "kvs.h"
#include "kvs_test_wrappers.h"
#include "../src/key_value_store/kvs_internal.h"

// --- Конфигурация теста ---
#define TEST_USER_DATA_SIZE (1024 * 512)
const char* KVS_STORAGE_FILE_PATH = "../data/kvs_storage.bin";

// --- Тестовые данные ---
#define NUM_TEST_KEYS 3
const char test_keys[NUM_TEST_KEYS][KVS_KEY_SIZE] = {"key_alpha", "key_beta", "key_gamma"};
const char* test_data[NUM_TEST_KEYS] = {"data_alpha_123", "data_beta_456", "data_gamma_789"};

typedef struct {
    uint8_t  key[KVS_KEY_SIZE];
    uint32_t value_offset;
    uint32_t value_size;
} TestMetadata;

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

void setup_clean_storage() {
    printf("\n--- Подготовка: Создание чистого хранилища с тестовыми данными ---\n");
    remove(KVS_STORAGE_FILE_PATH);
    Kvs_init(TEST_USER_DATA_SIZE);
    for (int i = 0; i < NUM_TEST_KEYS; ++i) {
        char key_buffer[KVS_KEY_SIZE] = {0};
        strncpy(key_buffer, test_keys[i], KVS_KEY_SIZE - 1);
        Kvs_put(key_buffer, KVS_KEY_SIZE, test_data[i], strlen(test_data[i]) + 1);
    }
    Kvs_deinit();
}

void check_all_keys_exist(bool should_exist) {
    printf("  Проверка доступности ключей:\n");
    char buffer[100];
    char key_buffer[KVS_KEY_SIZE];

    for (int i = 0; i < NUM_TEST_KEYS; ++i) {
        memset(key_buffer, 0, KVS_KEY_SIZE);
        strncpy(key_buffer, test_keys[i], KVS_KEY_SIZE - 1);

        Kvs_exists(key_buffer);

        if (should_exist) {
            size_t buffer_size = sizeof(buffer);
            Kvs_get(key_buffer, buffer, &buffer_size);
        }
    }
}

// --- Тестовые сценарии ---

void test_corrupt_main_superblock() {
    printf("\n--- Тест 1: Повреждение основного суперблока ---\n");
    setup_clean_storage();
    corrupt_file(0, sizeof(kvs_superblock));
    Kvs_init(TEST_USER_DATA_SIZE);
    check_all_keys_exist(true);
    Kvs_deinit();
}

void test_corrupt_data_bitmap() {
    printf("\n--- Тест 2: Повреждение Bitmap пользовательских данных ---\n");
    setup_clean_storage();
    kvs_superblock sb;
    FILE* fp = fopen(KVS_STORAGE_FILE_PATH, "rb");
    fread(&sb, sizeof(kvs_superblock), 1, fp);
    fclose(fp);
    corrupt_file(sb.bitmap_offset, 100);
    Kvs_init(TEST_USER_DATA_SIZE);
    check_all_keys_exist(true);
    Kvs_deinit();
}

void test_corrupt_metadata_bitmap() {
    printf("\n--- Тест 3: Повреждение Bitmap метаданных ---\n");
    setup_clean_storage();
    kvs_superblock sb;
    FILE* fp = fopen(KVS_STORAGE_FILE_PATH, "rb");
    fread(&sb, sizeof(kvs_superblock), 1, fp);
    fclose(fp);
    corrupt_file(sb.metadata_bitmap_offset, 4);
    Kvs_init(TEST_USER_DATA_SIZE);
    check_all_keys_exist(true);
    Kvs_deinit();
}

void test_corrupt_user_data() {
    printf("\n--- Тест 4: Повреждение данных одного ключа ---\n");
    setup_clean_storage();

    // Шаг 1: Прочитаем суперблок из файла, чтобы узнать реальную структуру хранилища
    kvs_superblock sb;
    FILE* fp_reader = fopen(KVS_STORAGE_FILE_PATH, "rb");
    if (!fp_reader || fread(&sb, sizeof(kvs_superblock), 1, fp_reader) != 1) {
        printf("  ПРОВЕРКА: Не удалось прочитать суперблок для теста. Тест пропущен.\n");
        if (fp_reader) fclose(fp_reader);
        return;
    }

    // Шаг 2: Находим, где лежат данные ключа 'key_beta'
    long data_to_corrupt_offset = 0;
    fseek(fp_reader, sb.metadata_offset, SEEK_SET);
    TestMetadata temp_meta;

    char key_buffer_for_cmp[KVS_KEY_SIZE];
    memset(key_buffer_for_cmp, 0, KVS_KEY_SIZE);
    strncpy(key_buffer_for_cmp, test_keys[1], KVS_KEY_SIZE - 1);

    for (uint32_t i = 0; i < sb.max_key_count; i++) {
        if (fread(&temp_meta, sizeof(TestMetadata), 1, fp_reader) != 1) break;
        if (memcmp(temp_meta.key, key_buffer_for_cmp, KVS_KEY_SIZE) == 0) {
            data_to_corrupt_offset = temp_meta.value_offset;
            break;
        }
    }
    fclose(fp_reader);

    if (data_to_corrupt_offset == 0) {
        printf("  ПРОВЕРКА: Не удалось найти смещение данных для повреждения. Тест пропущен.\n");
        return;
    }

    // Шаг 3: Повредим найденную область данных
    printf("  Целенаправленное повреждение данных ключа '%s' по смещению %ld.\n", test_keys[1], data_to_corrupt_offset);
    corrupt_file(data_to_corrupt_offset, 4);

    // Шаг 4: Проверяем результат
    Kvs_init(TEST_USER_DATA_SIZE);

    printf("  Проверка: Только поврежденный ключ ('%s') должен быть не найден.\n", test_keys[1]);

    char key_buffer[KVS_KEY_SIZE];

    // Этот ключ должен быть найден
    memset(key_buffer, 0, KVS_KEY_SIZE);
    strncpy(key_buffer, test_keys[0], KVS_KEY_SIZE - 1);
    Kvs_exists(key_buffer);

    // Этот ключ (поврежденный) не должен быть найден
    memset(key_buffer, 0, KVS_KEY_SIZE);
    strncpy(key_buffer, test_keys[1], KVS_KEY_SIZE - 1);
    Kvs_exists(key_buffer);

    // Этот ключ должен быть найден
    memset(key_buffer, 0, KVS_KEY_SIZE);
    strncpy(key_buffer, test_keys[2], KVS_KEY_SIZE - 1);
    Kvs_exists(key_buffer);

    Kvs_deinit();
}

void test_corrupt_both_superblocks() {
    printf("\n--- Тест 5: Повреждение обоих суперблоков ---\n");
    setup_clean_storage();
    kvs_superblock sb;
    FILE* fp = fopen(KVS_STORAGE_FILE_PATH, "rb");
    fread(&sb, sizeof(kvs_superblock), 1, fp);
    fclose(fp);

    corrupt_file(0, sb.superblock_size_bytes);
    corrupt_file(sb.superblock_backup_offset, sb.superblock_size_bytes);

    Kvs_init(TEST_USER_DATA_SIZE);

    printf("  Проверка состояния после сбоя инициализации:\n");
    check_all_keys_exist(false);
}

int main() {
    printf("=========================================================\n");
    printf("     ЗАПУСК ТЕСТА НА УСТОЙЧИВОСТЬ К ПОВРЕЖДЕНИЯМ      \n");
    printf("=========================================================\n");

    test_corrupt_main_superblock();
    test_corrupt_data_bitmap();
    test_corrupt_metadata_bitmap();
    test_corrupt_user_data();
    test_corrupt_both_superblocks();

    printf("\n=========================================================\n");
    printf("          ТЕСТИРОВАНИЕ УСТОЙЧИВОСТИ ЗАВЕРШЕНО         \n");
    printf("=========================================================\n");

    return 0;
}