#include "kvs_init.h"
#include "kvs_metadata.h"
#include "kvs_valid.h"
#include "kvs_internal_io.h"


kvs_internal_status kvs_setup_device(size_t user_size_bytes) {

    // Шаг 1: Выделяем память под основную управляющую структуру
    device = calloc(1, sizeof(kvs_device));
    if (!device) {
        kvs_log("Ошибка: не удалось выделить память под структуру устройства");
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    // Шаг 2: Получаем базовую геометрию из эмулятора SSD/MMC
    uint32_t global_page_count   = ssdmmc_sim_get_page_count();
    uint32_t words_per_page      = ssdmmc_sim_get_words_per_page();
    uint32_t word_size           = ssdmmc_sim_get_word_size();
    uint32_t page_size           = words_per_page * word_size;
    uint32_t superblock_size     = align_up(sizeof(kvs_superblock), word_size);
    uint32_t user_data_size      = align_up(user_size_bytes, word_size);
    uint32_t storage_size        = global_page_count * page_size;
    uint32_t superblock_backup_size = superblock_size;

    // Шаг 3: Итеративно рассчитываем размеры служебных областей
    uint32_t metadata_size = 0, prev_metadata_size = 0;
    uint32_t bitmap_bytes, metadata_bitmap_bytes, page_rewrite_bytes, crc_region_bytes;
    uint32_t total_words, total_page_count, crc_fixed_bytes;
    uint32_t max_keys, entry_crc_bytes;

    do {
        // На каждой итерации сохраняем предыдущий размер метаданных, чтобы понять, когда расчет стабилизируется
        prev_metadata_size = metadata_size;

        // Максимальное количество ключей напрямую зависит от размера области метаданных
        max_keys = (metadata_size > 0) ? (metadata_size / sizeof(kvs_metadata)) : 0;

        // Рассчитываем размер биткарты для данных пользователя (1 бит на слово)
        total_words = user_data_size / word_size;
        bitmap_bytes = align_up((total_words + 7) / 8, 4);

        // Рассчитываем размер биткарты для слотов метаданных (1 бит на слот)
        metadata_bitmap_bytes = align_up((max_keys + 7) / 8, 4);

        // Рассчитываем размер массива счетчиков перезаписи (для страниц userdata и metadata)
        total_page_count = (user_data_size + metadata_size + page_size - 1) / page_size;
        page_rewrite_bytes = align_up(total_page_count * sizeof(uint32_t), 4);

        // Рассчитываем общий размер области CRC
        crc_fixed_bytes = 5 * sizeof(uint32_t); // CRC для суперблоков, биткарт и счетчиков
        entry_crc_bytes = max_keys * sizeof(uint32_t); // Единый массив CRC для всех записей
        crc_region_bytes = align_up(crc_fixed_bytes + entry_crc_bytes, 4);

        // Финальный расчет размера области метаданных:
        // от всего пространства отнимаем все остальные области
        metadata_size = storage_size
                        - superblock_size
                        - superblock_backup_size
                        - bitmap_bytes
                        - metadata_bitmap_bytes
                        - page_rewrite_bytes
                        - crc_region_bytes
                        - user_data_size;
        metadata_size = align_up(metadata_size, 4);

    } while (metadata_size != prev_metadata_size); // Повторяем, пока размеры не перестанут меняться

    // Шаг 4: Заполняем структуру superblock всеми рассчитанными значениями
    device->superblock.magic                      = KVS_SUPERBLOCK_MAGIC;
    device->superblock.word_size_bytes            = word_size;
    device->superblock.userdata_size_bytes        = user_data_size;
    device->superblock.global_page_count          = global_page_count;
    device->superblock.words_per_page             = words_per_page;
    device->superblock.page_size_bytes            = page_size;
    device->superblock.storage_size_bytes         = storage_size;
    device->superblock.userdata_page_count        = (user_data_size + page_size - 1) / page_size;
    device->superblock.superblock_size_bytes      = superblock_size;
    device->superblock.bitmap_size_bytes          = bitmap_bytes;
    device->superblock.metadata_size_bytes        = metadata_size;
    device->superblock.max_key_count              = metadata_size / sizeof(kvs_metadata);
    device->superblock.metadata_bitmap_size_bytes = metadata_bitmap_bytes;
    device->superblock.last_data_word_checked     = 0;
    device->superblock.last_metadata_slot_checked = 0;
    device->superblock.bitmap_offset              = superblock_size;
    device->superblock.metadata_bitmap_offset     = superblock_size + bitmap_bytes;
    device->superblock.page_rewrite_offset        = superblock_size + bitmap_bytes + metadata_bitmap_bytes;
    device->superblock.page_crc_offset            = superblock_size + bitmap_bytes + metadata_bitmap_bytes + page_rewrite_bytes;
    device->superblock.data_offset                = superblock_size + bitmap_bytes + metadata_bitmap_bytes + page_rewrite_bytes + crc_region_bytes;
    device->superblock.metadata_offset            = superblock_size + bitmap_bytes + metadata_bitmap_bytes + page_rewrite_bytes + crc_region_bytes + user_data_size;
    device->superblock.superblock_backup_offset   = storage_size - superblock_backup_size;

    // Инициализируем указатели как NULL, на случай если выделение памяти далее провалится
    device->bitmap                     = NULL;
    device->metadata_bitmap            = NULL;
    device->page_rewrite_count         = NULL;
    device->page_crc.entry_crc         = NULL;
    device->key_index                  = NULL;

    // Проверяем, что в нашем хранилище будет место как минимум для KVS_MIN_NUM_METADATA метаданных
    if (device->superblock.max_key_count < KVS_MIN_NUM_METADATA) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_NO_FREE_METADATA_SPACE;
    }

    // Шаг 5: Выделяем память под все служебные массивы в ОЗУ
    device->bitmap                     = calloc(1, device->superblock.bitmap_size_bytes);
    device->metadata_bitmap            = calloc(1, device->superblock.metadata_bitmap_size_bytes);
    device->page_rewrite_count         = calloc(1, page_rewrite_bytes);
    device->page_crc.entry_crc         = calloc(1, device->superblock.max_key_count * sizeof(uint32_t));
    device->key_index                  = calloc(1, device->superblock.max_key_count * sizeof(kvs_key_index_entry));

    if (!device->bitmap || !device->metadata_bitmap || !device->page_rewrite_count || !device->page_crc.entry_crc || !device->key_index) {
        kvs_log("Ошибка: не удалось выделить память для служебных массивов");
        kvs_free_device();
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    device->key_count = 0;
    device->fp = NULL;
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_init_new(size_t storage_size_bytes) {

    kvs_log("Создание нового хранилища KVS (размер пользовательских данных: %lu байт)", storage_size_bytes);

    // Шаг 1: Настраиваем геометрию и выделяем память под структуры
    if (kvs_setup_device(storage_size_bytes) != KVS_INTERNAL_OK) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    // Шаг 2: Создаем и открываем файл хранилища
    device->fp = fopen(ssdmmc_sim_get_storage_filename(), "wb+");
    if (!device->fp) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_FILE_OPEN_FAILED;
    }

    // Шаг 3: Полностью стираем диск, заполняя его 0xFF
    if (ssdmmc_sim_format(device->fp) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_ERASE_FAILED;
    }

    // Шаг 4: Записываем на диск свежесозданный superblock
    if (kvs_write_region(device->fp, 0, &device->superblock, device->superblock.superblock_size_bytes) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    // Шаг 5: Записываем на диск резервную копию суперблока
    if (kvs_write_region(device->fp, device->superblock.superblock_backup_offset, &device->superblock, device->superblock.superblock_size_bytes) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    // Шаг 6: Записываем начальное состояние всех служебных структур
    if (kvs_persist_all_service_data() < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    kvs_log("Новое хранилище успешно создано и инициализировано");
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_load_existing(void) {

    kvs_log("Попытка загрузить существующее хранилище KVS");

    // Шаг 1: Пытаемся открыть файл
    FILE* fp = fopen(ssdmmc_sim_get_storage_filename(), "rb+");
    if (!fp) {
        return KVS_INTERNAL_ERR_FILE_OPEN_FAILED;
    }

    // Шаг 2: Создаем временную структуру для безопасного чтения
    device = calloc(1, sizeof(kvs_device));
    if (!device) {
        fclose(fp);
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    device->fp = fp;
    device->superblock.word_size_bytes = ssdmmc_sim_get_word_size();
    device->superblock.words_per_page  = ssdmmc_sim_get_words_per_page();
    device->superblock.global_page_count = ssdmmc_sim_get_page_count();
    device->superblock.page_size_bytes = device->superblock.word_size_bytes * device->superblock.words_per_page;
    device->page_rewrite_count         = NULL;
    device->metadata_bitmap            = NULL;
    device->key_index                  = NULL;
    device->bitmap                     = NULL;
    device->page_crc.entry_crc         = NULL;

    // Шаг 3: Читаем оба суперблока с диска
    uint32_t superblock_size = align_up(sizeof(kvs_superblock), device->superblock.word_size_bytes);
    uint32_t storage_size = device->superblock.page_size_bytes * device->superblock.global_page_count;
    uint32_t backup_offset = storage_size - superblock_size;

    kvs_superblock primary_sb, backup_sb;
    if (kvs_read_region(device->fp, backup_offset, &backup_sb, superblock_size) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    if (kvs_read_region(device->fp, 0, &primary_sb, superblock_size) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Шаг 4: Проверяем валидность обоих суперблоков
    uint32_t primary_sb_crc = 0, backup_sb_crc = 0;
    kvs_read_region(device->fp, primary_sb.page_crc_offset, &primary_sb_crc, sizeof(uint32_t));
    kvs_read_region(device->fp, backup_sb.page_crc_offset + sizeof(uint32_t), &backup_sb_crc, sizeof(uint32_t));

    bool primary_valid = (primary_sb_crc == crc32_calc(&primary_sb, sizeof(kvs_superblock)));
    bool backup_valid = (backup_sb_crc == crc32_calc(&backup_sb, sizeof(kvs_superblock)));

    // Шаг 5: Выбираем, какой суперблок использовать
    if (primary_valid) {
        device->superblock = primary_sb;
    } else if (backup_valid) {
        kvs_log("ВНИМАНИЕ: Основной суперблок поврежден! Восстанавливаем из резервной копии.");
        device->superblock = backup_sb;
        if (kvs_write_region(device->fp, 0, &device->superblock, device->superblock.superblock_size_bytes) < 0) {
            kvs_log("КРИТИЧЕСКАЯ ОШИБКА: Не удалось восстановить основной суперблок.");
            kvs_free_device();
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }
    } else {
        kvs_log("ОШИБКА: Оба суперблока повреждены. Хранилище не может быть загружено.");
        kvs_free_device();
        return KVS_INTERNAL_ERR_CORRUPT_SUPERBLOCK;
    }


    // Шаг 6: Теперь, когда у нас есть валидный суперблок, выделяем память и читаем служебные данные
    device->page_crc.entry_crc = calloc(1, device->superblock.max_key_count * sizeof(uint32_t));
    if (!device->page_crc.entry_crc) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    if (kvs_read_crc_info() < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    uint32_t rewrite_size = device->superblock.page_crc_offset - device->superblock.page_rewrite_offset;
    device->bitmap = calloc(1, device->superblock.bitmap_size_bytes);
    device->metadata_bitmap = calloc(1, device->superblock.metadata_bitmap_size_bytes);
    device->page_rewrite_count = calloc(1, rewrite_size);
    device->key_index = calloc(1, device->superblock.max_key_count * sizeof(kvs_key_index_entry));
    if (!device->bitmap || !device->metadata_bitmap || !device->page_rewrite_count || !device->key_index) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    if (kvs_read_region(device->fp, device->superblock.bitmap_offset, device->bitmap, device->superblock.bitmap_size_bytes) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }
    if (kvs_read_region(device->fp, device->superblock.metadata_bitmap_offset, device->metadata_bitmap, device->superblock.metadata_bitmap_size_bytes) < 0) {
        kvs_free_device(); return KVS_INTERNAL_ERR_READ_FAILED;
    }
    if (kvs_read_region(device->fp, device->superblock.page_rewrite_offset, device->page_rewrite_count, rewrite_size) < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Шаг 7: Проверяем и при необходимости восстанавливаем служебные области
    if (is_metadata_bitmap_valid() != 1) {
        kvs_log("Биткарта метаданных повреждена, пересоздаем...");
        if (kvs_metadata_bitmap_create() < 0) {
            kvs_free_device();
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }
    }

    // Шаг 8: Строим key_index ключей в ОЗУ.
    if (build_key_index() < 0) {
        kvs_free_device();
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Шаг 9: Теперь, имея надежный key_index, проверяем и восстанавливаем битовую карту данных.
    if (is_bitmap_valid() != 1) {
        kvs_log("Биткарта данных повреждена, пересоздаем...");
        if (kvs_bitmap_create() < 0) {
            kvs_free_device();
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }
    }

    if (is_page_rewrite_count_valid() != 1) {
        kvs_log("Счетчики перезаписи повреждены, сбрасываем...");
        if (kvs_clear_region(device->fp, device->superblock.page_rewrite_offset, rewrite_size) < 0) {
            kvs_free_device();
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }
    }

    kvs_log("Существующее хранилище успешно загружено и проверено");
    return KVS_INTERNAL_OK;
}

kvs_status kvs_init(size_t storage_size_bytes)
{
    // Выравниваем запрошенный размер до размера слова
    uint32_t word_size = ssdmmc_sim_get_word_size();
    size_t real_storage_size_bytes = align_up(storage_size_bytes,word_size);

    // Проверяем, не была ли библиотека уже инициализирована
    if (device) {
        kvs_log("Предупреждение: KVS уже инициализирован.");
        return KVS_ERROR_ALREADY_INITIALIZED;
    }

    // Создаем директорию для хранения данных, если ее нет
    if (ssdmmc_sim_ensure_data_dir_exists() != SSDMMC_OK) {
        kvs_log("КРИТИЧЕСКАЯ ОШИБКА: Не удалось создать директорию для данных.");
        return KVS_ERROR_STORAGE_FAILURE;
    }

    // Записываем разделитель в лог-файл для удобства чтения
    FILE *log_fp = fopen(KVS_LOG_FILENAME,"a");
    if(log_fp){
        fprintf(log_fp,"\n------------------------------ НОВЫЙ ЗАПУСК ------------------------------\n\n");
        fflush(log_fp);
        fclose(log_fp);
    }

    // Пытаемся загрузить существующее хранилище
    if (kvs_load_existing() == KVS_INTERNAL_OK) {
        kvs_log("Инициализация завершена: загружено существующее хранилище");
        return KVS_SUCCESS;
    }


    if (kvs_init_new(real_storage_size_bytes) == KVS_INTERNAL_OK) {
        kvs_log("Инициализация завершена: создано новое хранилище");
        return KVS_SUCCESS;
    }

    // Если произошла любая другая ошибка при загрузке (повреждение и т.д.), сообщаем о сбое
    kvs_log("Ошибка: не удалось инициализировать KVS из-за повреждения или другой ошибки.");
    kvs_free_device();
    return KVS_ERROR_STORAGE_FAILURE;
}

void kvs_deinit(void)
{
    // Если устройство не было инициализировано, ничего не делаем
    if (!device) {
        return;
    }

    // Сохраняем все служебные данные и освобождаем ресурсы
    kvs_log("Деинициализация KVS...");
    if (kvs_persist_all_service_data() < 0) {
        kvs_log("Внимание: не удалось сохранить финальное состояние служебных данных перед деинициализацией.");
    }
    kvs_free_device();
    kvs_log("Деинициализация завершена.");
}