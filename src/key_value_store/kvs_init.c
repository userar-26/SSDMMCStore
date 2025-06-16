#include "kvs_init.h"


int kvs_setup_device(size_t user_size_bytes) {

    device = malloc(sizeof(kvs_device));
    if (!device) {
        kvs_log("Ошибка: не удалось выделить память под структуру устройства");
        return -1;
    }

    // Исходные данные
    uint32_t global_page_count   = ssdmmc_sim_get_page_count();
    uint32_t words_per_page      = ssdmmc_sim_get_words_per_page();
    uint32_t word_size           = ssdmmc_sim_get_word_size();
    uint32_t page_size           = words_per_page * word_size;
    uint32_t superblock_size     = KVS_SUPERBLOCK_SIZE;
    uint32_t user_data_size      = align_up(user_size_bytes, word_size);
    uint32_t storage_size        = global_page_count * words_per_page * word_size;

    // Вспомогательные переменные
    uint32_t metadata_size = 0, prev_metadata_size = 0;
    uint32_t bitmap_bytes, page_rewrite_bytes, crc_region_bytes;
    uint32_t total_words, total_page_count, user_page_count, page_crc_bytes, crc_fixed_bytes;

    do {
        prev_metadata_size = metadata_size;

        // 1. Рассчитываем битовую карту (1 бит на слово пользовательских данных)
        total_words = user_data_size / word_size; // Сколько слов занимают данные пользователя
        bitmap_bytes = (total_words + 7) / 8;     // Байт для хранения битов (округление вверх)
        bitmap_bytes = align_up(bitmap_bytes, 4); // Выравнивание по 4 байта

        // 2. Рассчитываем массив перезаписей страниц (1 uint32_t на страницу)
        //    Учитываем ВСЕ страницы (данные + метаданные)
        total_page_count = (user_data_size + metadata_size + page_size - 1) / page_size;
        page_rewrite_bytes = total_page_count * sizeof(uint32_t); // 4 байта на страницу
        page_rewrite_bytes = align_up(page_rewrite_bytes, 4);     // Выравнивание по 4 байта

        // 3. Рассчитываем CRC-регион
        //    - Фиксированный CRC (например, 4 uint32_t = 16 байт)
        crc_fixed_bytes = 4 * sizeof(uint32_t);
        //    - CRC для каждой страницы пользовательских данных (1 uint32_t на страницу)
        user_page_count = (user_data_size + page_size - 1) / page_size;
        page_crc_bytes = user_page_count * sizeof(uint32_t);
        //    - Общий размер CRC-региона
        crc_region_bytes = crc_fixed_bytes + page_crc_bytes;
        crc_region_bytes = align_up(crc_region_bytes, 4); // Выравнивание по 4 байта

        // 4. Пересчитываем размер метаданных
        metadata_size = storage_size
                        - superblock_size
                        - bitmap_bytes
                        - page_rewrite_bytes
                        - crc_region_bytes
                        - user_data_size;
        metadata_size = align_up(metadata_size, 4); // Выравнивание по 4 байта

    } while (metadata_size != prev_metadata_size); // Итеративно уточняем размер


    // Заполнение superblock
    device->superblock.magic                = KVS_SUPERBLOCK_MAGIC;
    device->superblock.word_size_bytes      = word_size;
    device->superblock.userdata_size_bytes  = user_data_size;
    device->superblock.global_page_count    = global_page_count;
    device->superblock.words_per_page       = words_per_page;
    device->superblock.page_size_bytes      = page_size;
    device->superblock.storage_size_bytes   = storage_size;
    device->superblock.userdata_page_count  = user_page_count;
    device->superblock.superblock_size_bytes= superblock_size;
    device->superblock.bitmap_size_bytes    = bitmap_bytes;
    device->superblock.metadata_size_bytes  = metadata_size;
    device->superblock.max_key_count        = metadata_size / sizeof(kvs_metadata);

    device->superblock.bitmap_offset        = superblock_size;
    device->superblock.page_rewrite_offset  = superblock_size + bitmap_bytes;
    device->superblock.page_crc_offset      = superblock_size + bitmap_bytes + page_rewrite_bytes;
    device->superblock.data_offset          = superblock_size + bitmap_bytes + page_rewrite_bytes + crc_region_bytes;
    device->superblock.metadata_offset      = superblock_size + bitmap_bytes + page_rewrite_bytes + crc_region_bytes + user_data_size;

    // Проверяем, что в созданном хранилище можно будет хранить определенный минимум метаданных
    if (device->superblock.max_key_count < KVS_MIN_NUM_METADATA) {
        kvs_log("Ошибка: недостаточно памяти для хранения минимального количества метаданных (max_key_count=%u, требуется минимум %u)",
                device->superblock.max_key_count, KVS_MIN_NUM_METADATA);
        kvs_free_device();
        return -2;
    }

    // Выделение памяти для битовой карты
    device->bitmap = malloc(device->superblock.bitmap_size_bytes);
    if (!device->bitmap) {
        kvs_log("Ошибка: не удалось выделить память для битовой карты (bitmap_size_bytes=%u)", device->superblock.bitmap_size_bytes);
        kvs_free_device();
        return -3;
    }

    // Выделение памяти для массива счетчиков очисток (для всех страниц: userdata + metadata)
    device->page_rewrite_count = malloc(total_page_count * sizeof(*device->page_rewrite_count));
    if (!device->page_rewrite_count) {
        kvs_log("Ошибка: не удалось выделить память для массива счетчиков перезаписей страниц (total_page_count=%u)", total_page_count);
        kvs_free_device();
        return -4;
    }

    // Выделение памяти для массива CRC страниц (только для пользовательских страниц)
    device->page_crc.page_crc = malloc(user_page_count * sizeof(*device->page_crc.page_crc));
    if (!device->page_crc.page_crc) {
        kvs_log("Ошибка: не удалось выделить память для массива CRC страниц (user_page_count=%u)", user_page_count);
        kvs_free_device();
        return -5;
    }

    // Выделение памяти для индекса ключей
    device->key_index = malloc(device->superblock.max_key_count * sizeof(*device->key_index));
    if (!device->key_index) {
        kvs_log("Ошибка: не удалось выделить память для массива индекса ключей (max_key_count=%u)", device->superblock.max_key_count);
        kvs_free_device();
        return -6;
    }

    device->key_count = 0;
    device->fp        = NULL;
    return 0;
}

int kvs_init_new(size_t storage_size_bytes) {

    kvs_log("Создание нового хранилища KVS (размер пользовательских данных: %zu байт)", storage_size_bytes);

    if (device) {
        kvs_free_device();
    }

    if (kvs_setup_device(storage_size_bytes) != 0)
        return -1;

    device->fp = fopen(KVS_STORAGE_FILENAME, "wb+");
    if (!device->fp) {
        kvs_log("Ошибка: не удалось создать файл хранилища: %s", KVS_STORAGE_FILENAME);
        kvs_free_device();
        return -2;
    }

    if (ssdmmc_sim_format(device->fp) < 0) {
        kvs_log("Ошибка: не удалось отформатировать новое хранилище");
        kvs_free_device();
        return -3;
    }

    if (kvs_write_region(device->fp, 0, &device->superblock, device->superblock.superblock_size_bytes) < 0) {
        kvs_log("Ошибка: не удалось записать superblock в новое хранилище");
        kvs_free_device();
        return -4;
    }

    // После записи superblock пересчитываем значения CRC всех служебных данных
    if (kvs_update_all_service_crc() < 0) {
        kvs_log("Ошибка: не удалось пересчитать CRC служебных областей");
        kvs_free_device();
        return -5;
    }

    if (kvs_write_region(device->fp, device->superblock.page_crc_offset, &device->page_crc, sizeof(device->page_crc)) < 0) {
        kvs_log("Ошибка: не удалось записать CRC служебных областей");
        kvs_free_device();
        return -6;
    }

    kvs_log("Новое хранилище успешно создано и инициализировано");
    return 0;
}

int kvs_load_existing(void) {

    kvs_log("Попытка загрузить существующее хранилище KVS");

    // Если устройство уже инициализировано, освобождаем память
    if (device) {
        kvs_free_device();
    }

    // Выделяем память под структуру устройства
    device = malloc(sizeof(kvs_device));
    if (!device) {
        kvs_log("Ошибка: не удалось выделить память под структуру устройства");
        return -1;
    }
    memset(device, 0, sizeof(kvs_device));

    // Открываем файл хранилища для чтения и записи
    device->fp = fopen(KVS_STORAGE_FILENAME, "rb+");
    if (!device->fp) {
        kvs_log("Файл хранилища не найден: %s", KVS_STORAGE_FILENAME);
        kvs_free_device();
        return -2;
    }

    // 1. Читаем superblock из файла
    if (kvs_read_region(device->fp, 0, &device->superblock, sizeof(device->superblock)) < 0) {
        kvs_log("Ошибка: не удалось прочитать superblock из файла");
        kvs_free_device();
        return -3;
    }

    // 2. Читаем CRC-структуру (kvs_crc_info) из файла
    memset(&device->page_crc, 0, sizeof(device->page_crc));
    device->page_crc.page_crc = malloc(device->superblock.userdata_page_count * sizeof(uint32_t));
    if (!device->page_crc.page_crc) {
        kvs_log("Ошибка: не удалось выделить память для массива CRC страниц");
        kvs_free_device();
        return -4;
    }

    if (kvs_read_crc_info(device->fp, device->superblock.page_crc_offset, &device->page_crc, device->superblock.userdata_page_count) < 0) {
        kvs_log("Ошибка: не удалось прочитать структуру CRC из файла");
        kvs_free_device();
        return -5;
    }

    // 3. Проверяем валидность superblock (магическое число, размеры и т.д.)
    if (!is_superblock_valid(device)) {
        kvs_log("Ошибка: superblock невалиден, требуется пересоздание хранилища");
        kvs_free_device();
        return -6;
    }

    // 4. Выделяем память под служебные массивы:
    uint32_t total_page_count = (device->superblock.userdata_size_bytes +
                                 device->superblock.metadata_size_bytes +
                                 device->superblock.page_size_bytes - 1) /
                                device->superblock.page_size_bytes;

    device->bitmap = malloc(device->superblock.bitmap_size_bytes * sizeof(*device->bitmap));
    device->page_rewrite_count = malloc(total_page_count * sizeof(*device->page_rewrite_count));
    device->key_index = malloc(device->superblock.max_key_count * sizeof(*device->key_index));
    device->key_count = 0;

    if (!device->bitmap || !device->page_rewrite_count || !device->key_index) {
        kvs_log("Ошибка: не удалось выделить память для служебных массивов");
        kvs_free_device();
        return -7;
    }

    // 5. Читаем содержимое служебных областей из файла в память

    // 5.1. Читаем битовую карту
    if (kvs_read_region(device->fp, device->superblock.bitmap_offset, device->bitmap, device->superblock.bitmap_size_bytes) < 0) {
        kvs_log("Ошибка: не удалось прочитать битовую карту из файла");
        kvs_free_device();
        return -8;
    }

    // 5.2. Читаем массив счетчиков перезаписей страниц
    if (kvs_read_region(device->fp, device->superblock.page_rewrite_offset,
                        device->page_rewrite_count,
                        total_page_count * sizeof(*device->page_rewrite_count)) < 0) {
        kvs_log("Ошибка: не удалось прочитать массив счетчиков перезаписей страниц из файла");
        kvs_free_device();
        return -9;
    }

    // 5.3. Проверяем валидность массива счетчиков перезаписей страниц
    if (!is_page_rewrite_count_valid(device)) {
        if(kvs_clear_region(device->fp,device->superblock.page_rewrite_offset,total_page_count * sizeof(*device->page_rewrite_count))){
            kvs_log("Ошибка: не удалось очистить массив счетчиков перезаписей страниц");
            kvs_free_device();
            return -9;
        }
    }

    // 6. Восстановление метаданных и построение key_index

    // Проверяем валидность области метаданных
    if (!is_metadata_valid(device)) {

        kvs_log("Область метаданных повреждена или невалидна, выполняется восстановление");

        if( kvs_gc() == 0){
            kvs_log("Ошибка: после проведения очистки невалидных данных, таковы не были удалены");
            kvs_free_device();
            return -10;
        }
    } else {
        // Если метаданные валидны, строим key_index по ним
        if (build_key_index() < 0) {
            kvs_free_device();
            return -18;
        }
    }

    // 7. Проверяем валидность битовой карты
    if (!is_bitmap_valid(device)){

        if (kvs_clear_region(device->fp,device->superblock.bitmap_offset,device->superblock.bitmap_size_bytes)){
            kvs_log("Ошибка: не удалось очистить битовую карту");
            kvs_free_device();
            return -9;
        }

        // Битовая карта не валидна, поэтому пытаемся создать новую
        if ( kvs_bitmap_create() < 0 ){
            kvs_log("Ошибка: не удалось создать битовую карту");
            kvs_free_device();
            return -9;
        }

    }

    kvs_log("Существующее хранилище успешно загружено и проверено");
    return 0;
}

int kvs_init(size_t storage_size_bytes)
{
    // Создаем директорию в которой будет храниться наше хранилище, если ее нету
    kvs_make_data_dir();

    // Пытаемся загрузить существующее хранилище
    int rc = kvs_load_existing();
    if (rc == 0) {
        kvs_log("Инициализация завершена: загружено существующее хранилище");
        return 0;
    }

    kvs_log("Существующее хранилище не найдено или повреждено, создаем новое");
    rc = kvs_init_new(storage_size_bytes);
    if (rc == 0) {
        kvs_log("Инициализация завершена: создано новое хранилище");
        return 0;
    }

    kvs_log("Ошибка: не удалось инициализировать KVS ни как новое, ни как существующее");
    return rc;
}

void kvs_deinit(void)
{
    // Проверяем, инициализировано ли устройство
    if (!device) {
        kvs_log("Деинициализация: устройство уже освобождено или не было инициализировано");
        return;
    }

    kvs_log("Деинициализация: освобождаем ресурсы KVS");

    // Освобождаем все ресурсы, связанные с устройством
    kvs_free_device();


    kvs_log("Деинициализация завершена: все ресурсы освобождены");
}
