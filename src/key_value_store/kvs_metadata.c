#include "kvs_metadata.h"
#include "kvs_internal_io.h"
#include "kvs_valid.h"

uint32_t crc32_calc(const void *data, size_t size)
{
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < size; i++) {
        crc ^= buf[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }

    return ~crc;
}

kvs_internal_status crc_update_region(uint32_t offset, uint32_t size)
{

    // Шаг 1: Проверяем базовые параметры
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (offset < device->superblock.data_offset || offset + size > device->superblock.metadata_offset) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Шаг 2: Получаем геометрию страниц из суперблока
    uint32_t page_size           = device->superblock.page_size_bytes;
    uint32_t userdata_start_page = device->superblock.data_offset / page_size;

    // Шаг 3: Вычисляем диапазон страниц, которые нужно обновить
    uint32_t first_page = offset / page_size;
    uint32_t last_page  = (offset + size - 1) / page_size;

    // Шаг 4: Выделяем временный буфер для чтения одной страницы
    uint8_t *temp_page = calloc(1,page_size);
    if (!temp_page) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }

    // Шаг 5: В цикле проходим по каждой затронутой странице
    for (uint32_t i = first_page; i <= last_page; i++) {

        // Считываем всю страницу в буфер
        uint32_t current_page_offset = i * page_size;
        if (kvs_read_region(device->fp, current_page_offset, temp_page, page_size) < 0) {
            free(temp_page);
            return KVS_INTERNAL_ERR_READ_FAILED;
        }

        // Вычисляем CRC для содержимого страницы
        uint32_t temp_crc = crc32_calc(temp_page, page_size);

        // Вычисляем правильный индекс в массиве CRC и обновляем значение
        uint32_t crc_index = i - userdata_start_page;
        device->page_crc.page_crc[crc_index] = temp_crc;
    }

    // Шаг 6: Освобождаем временный буфер
    free(temp_page);

    return KVS_INTERNAL_OK;
}

kvs_internal_status rewrite_count_increment_region(uint32_t offset, uint32_t size)
{
    // Проверяем базовые условия
    if (!device)
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    if (offset + size > device->superblock.storage_size_bytes)
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    if (size == 0)
        return KVS_INTERNAL_OK;

    // Вычисляем диапазон страниц, счетчики для которых нужно увеличить
    uint32_t start_tracked_page = device->superblock.data_offset / device->superblock.page_size_bytes;
    uint32_t first_page = offset / device->superblock.page_size_bytes;
    uint32_t last_page  = (offset + size - 1) / device->superblock.page_size_bytes;

    // Увеличиваем счетчики для вычисленных страниц
    for (uint32_t i = first_page; i <= last_page; i++) {
        uint32_t rewrite_index = i - start_tracked_page;
        device->page_rewrite_count[rewrite_index]++;
    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status bitmap_set_region(uint32_t offset, uint32_t size)
{
    // Делаем базовую проверку
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (offset < device->superblock.data_offset || offset + size > device->superblock.metadata_offset) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    if (size == 0) {
        return KVS_INTERNAL_OK;
    }

    // Вычисляем начальное слово и количество слов для пометки
    uint32_t word_size  = device->superblock.word_size_bytes;
    uint32_t start_word = (offset - device->superblock.data_offset) / word_size;
    uint32_t num_words  = (size + word_size - 1) / word_size;

    // В цикле устанавливаем каждый соответствующий бит
    for (uint32_t i = 0; i < num_words; i++) {
        uint32_t current_word_index = start_word + i;
        uint32_t byte_index = current_word_index / 8;
        uint8_t  bit_index  = current_word_index % 8;
        device->bitmap[byte_index] |= (1 << bit_index);
    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status bitmap_clear_region(uint32_t offset, uint32_t size)
{
    // Делаем базовую проверку
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (offset < device->superblock.data_offset || offset + size > device->superblock.metadata_offset) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    if (size == 0) {
        return KVS_INTERNAL_OK;
    }

    // Вычисляем начальное слово и количество слов для очистки
    uint32_t word_size  = device->superblock.word_size_bytes;
    uint32_t start_word = (offset - device->superblock.data_offset) / word_size;
    uint32_t num_words  = (size + word_size - 1) / word_size;

    // В цикле сбрасываем каждый соответствующий бит
    for (uint32_t i = 0; i < num_words; i++) {
        uint32_t current_word_index = start_word + i;
        uint32_t byte_index = current_word_index / 8;
        uint8_t  bit_index  = current_word_index % 8;
        device->bitmap[byte_index] &= ~(1 << bit_index);
    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_bitmap_create(void)
{

    if(!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Сначала полностью очищаем битовую карту в памяти
    memset(device->bitmap, 0, device->superblock.bitmap_size_bytes);
    if (device->key_count == 0) {
        return KVS_INTERNAL_OK;
    }

    kvs_metadata temp;
    // Проходим по всем валидным ключам в key_index
    for(int i = 0; i < device->key_count; i++)
    {
        // Для каждого ключа читаем его метаданные, чтобы узнать, где лежат его данные
        if (kvs_read_region(device->fp, device->key_index[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0) {
            return KVS_INTERNAL_ERR_READ_FAILED;
        }
        // Помечаем область данных этого ключа как занятую
        if(bitmap_set_region(temp.value_offset, temp.value_size) < 0) {
            return KVS_INTERNAL_ERR_WRITE_FAILED;
        }
    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status build_key_index(void) {

    // Делаем базовую проверку
    if(!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Обнуляем текущее значение ключей перед построением
    device->key_count = 0;

    kvs_metadata temp;
    // Проходим по всем возможным слотам метаданных
    for (uint32_t i = 0; i < device->superblock.max_key_count; i++) {

        // Используем биткарту метаданных для быстрой проверки: если слот свободен, пропускаем его.
        if (!get_bit(device->metadata_bitmap, i)) {
            continue;
        }
        // Вычисляем физическое смещение слота напрямую по его индексу i.
        uint32_t current_position = device->superblock.metadata_offset + (i * sizeof(kvs_metadata));

        // Если бит установлен, читаем слот с диска
        if (kvs_read_region(device->fp, current_position, &temp, sizeof(temp)) < 0) {
            return KVS_INTERNAL_ERR_READ_FAILED;
        }

        // Проверяем, является ли запись в слоте полностью валидной
        if (is_metadata_entry_valid(&temp)) {
            // Если да, добавляем ее в key_index в ОЗУ
            kvs_add_metadata_entry(&temp, current_position);
        }
    }

    // После того как все валидные ключи добавлены, сортируем key_index для быстрого поиска
    if (device->key_count > 0) {
        qsort(device->key_index, device->key_count, sizeof(kvs_key_index_entry), kvs_key_index_entry_cmp);
    }

    return KVS_INTERNAL_OK;
}

int kvs_key_index_entry_cmp(const void *a, const void *b) {
    const kvs_key_index_entry *temp_a = a;
    const kvs_key_index_entry *temp_b = b;
    return memcmp(temp_a->key, temp_b->key, KVS_KEY_SIZE);
}

int get_bit(const uint8_t *bitmap, uint32_t bit)
{
    return (bitmap[bit / 8] >> (bit % 8)) & 1;
}

uint32_t kvs_find_free_data_offset(uint32_t value_len)
{
    // Выполняем базовую проверку
    if (!device) {
        return UINT32_MAX;
    }

    // Рассчитываем то количество слов, которое нам нужно выделить
    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t total_words = device->superblock.userdata_size_bytes / word_size;
    uint32_t words_needed = (value_len + word_size - 1) / word_size;

    if (words_needed > total_words) {
        return UINT32_MAX;
    }

    // Будем делать два прохода, для реализации метода выравнивания путем карусели
    uint32_t start_scan_idx = device->superblock.last_data_word_checked;
    uint32_t run_length = 0;

    // Проход 1: От последнего найденного места до конца
    for (uint32_t i = start_scan_idx; i < total_words;i++) {
        if (get_bit(device->bitmap, i) == 0) {
            run_length++;
        } else {
            // Сбрасываем счетчик, если встретили занятый бит
            run_length = 0;
        }

        if (run_length >= words_needed) {
            uint32_t block_start_idx = i - (words_needed - 1);
            device->superblock.last_data_word_checked = i;
            return device->superblock.data_offset + block_start_idx * word_size;
        }
    }

    // Проход 2: От начала до последнего найденного места (если в первом проходе не нашли)
    if (start_scan_idx > 0) {
        run_length = 0;
        for (uint32_t i = 0; i < start_scan_idx; i++) {
            if (get_bit(device->bitmap, i) == 0) {
                run_length++;
            } else {
                run_length = 0;
            }
            if (run_length >= words_needed) {
                uint32_t block_start_idx = i - (words_needed - 1);
                device->superblock.last_data_word_checked = i;
                return device->superblock.data_offset + block_start_idx * word_size;
            }
        }
    }

    // Если после двух проходов ничего не найдено
    return UINT32_MAX;
}

uint32_t kvs_find_free_metadata_offset(void)
{
    // Делаем базовую проверку
    if (!device) {
        return UINT32_MAX;
    }

    uint32_t total_slots = device->superblock.max_key_count;

    // Начинаем поиск с последнего выделенного слота
    uint32_t start_slot = device->superblock.last_metadata_slot_checked;

    // Проходим по всей биткарте один раз (по кругу)
    for (uint32_t i = 0; i < total_slots; i++) {

        uint32_t current_slot = (start_slot + i) % total_slots;

        if (!get_bit(device->metadata_bitmap, current_slot)) {

            // Если слот свободен, обновляем карусель и возвращаем его смещение
            device->superblock.last_metadata_slot_checked = current_slot;
            return device->superblock.metadata_offset + (current_slot * sizeof(kvs_metadata));

        }
    }

    return UINT32_MAX;
}

kvs_internal_status kvs_persist_all_service_data(void)
{
    // Делаем базовую проверку
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    // Шаг 1: Пересчитываем CRC для всех служебных областей перед записью
    device->page_crc.superblock_crc        = crc32_calc(&device->superblock, sizeof(kvs_superblock));
    device->page_crc.superblock_backup_crc = device->page_crc.superblock_crc;
    device->page_crc.bitmap_crc            = crc32_calc(device->bitmap, device->superblock.bitmap_size_bytes);
    device->page_crc.metadata_bitmap_crc   = crc32_calc(device->metadata_bitmap, device->superblock.metadata_bitmap_size_bytes);
    uint32_t rewrite_size                  = device->superblock.page_crc_offset - device->superblock.page_rewrite_offset;
    device->page_crc.rewrite_crc           = crc32_calc(device->page_rewrite_count, rewrite_size);

    // Шаг 2: Последовательно записываем каждую служебную область
    if (kvs_write_region(device->fp, 0, &device->superblock, sizeof(kvs_superblock)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    if (kvs_write_region(device->fp, device->superblock.superblock_backup_offset, &device->superblock, sizeof(kvs_superblock)) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    if (kvs_write_region(device->fp, device->superblock.bitmap_offset, device->bitmap, device->superblock.bitmap_size_bytes) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    if (kvs_write_region(device->fp, device->superblock.metadata_bitmap_offset, device->metadata_bitmap, device->superblock.metadata_bitmap_size_bytes) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }
    if (kvs_write_region(device->fp, device->superblock.page_rewrite_offset, device->page_rewrite_count, rewrite_size) < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    // Шаг 3: Записываем всю обновленную структуру CRC
    if (kvs_write_crc_info() < 0) {
        return KVS_INTERNAL_ERR_WRITE_FAILED;
    }

    // Шаг 4: Принудительно сбрасываем буферы файла на диск
    fflush(device->fp);
    return KVS_INTERNAL_OK;
}

kvs_internal_status bitmap_set_metadata_slot(uint32_t slot_index)
{
    if (!device || slot_index >= device->superblock.max_key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    uint32_t byte_index = slot_index / 8;
    uint8_t bit_index = slot_index % 8;
    device->metadata_bitmap[byte_index] |= (1 << bit_index);
    return KVS_INTERNAL_OK;
}

kvs_internal_status bitmap_clear_metadata_slot(uint32_t slot_index)
{
    if (!device || slot_index >= device->superblock.max_key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }
    uint32_t byte_index = slot_index / 8;
    uint8_t bit_index = slot_index % 8;
    device->metadata_bitmap[byte_index] &= ~(1 << bit_index);
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_metadata_bitmap_create(void)
{

    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }

    memset(device->metadata_bitmap, 0, device->superblock.metadata_bitmap_size_bytes);
    // Проходим по всей области метаданных, и если область размером равным размеру слота метаданных
    // не пуста, то добавляем данную область в биткарту метаданных
    for (uint32_t i = 0; i < device->superblock.max_key_count; i++) {

        uint32_t slot_offset = device->superblock.metadata_offset + (i * sizeof(kvs_metadata));
        int empty_check = is_data_region_empty(slot_offset, sizeof(kvs_metadata));

        if (empty_check < 0) {
            return empty_check;
        }
        if (empty_check == 0) {
            bitmap_set_metadata_slot(i);
        }

    }
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_add_metadata_entry(const kvs_metadata *new_metadata, uint32_t pos)
{
    // Делаем базовую проверку
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (new_metadata == NULL) {
        return KVS_INTERNAL_ERR_NULL_PARAM;
    }
    if (device->key_count >= device->superblock.max_key_count) {
        return KVS_INTERNAL_ERR_KEY_INDEX_FULL;
    }

    // Добавляем метаданные в key_index
    kvs_key_index_entry temp;
    memcpy(temp.key, new_metadata->key, KVS_KEY_SIZE);
    temp.metadata_offset = pos;
    temp.flags = 1;
    device->key_index[device->key_count++] = temp;
    return KVS_INTERNAL_OK;
}

kvs_internal_status kvs_update_single_metadata_crc(uint32_t slot_index) {

    // Делаем базовую проверку
    if (!device || slot_index >= device->superblock.max_key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Считываем метаданные, соответствующие слоту с индексом slot_index
    kvs_metadata temp_metadata;
    uint32_t slot_offset = device->superblock.metadata_offset + (slot_index * sizeof(kvs_metadata));
    if (kvs_read_region(device->fp, slot_offset, &temp_metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Записываем полученный CRC для данного слота в соответствующий массив
    device->page_crc.metadata_slot_crc[slot_index] = crc32_calc(&temp_metadata, sizeof(kvs_metadata));
    return KVS_INTERNAL_OK;
}

uint32_t kvs_find_victim_page(int clean_mod, const uint8_t *valid_bitmap, uint32_t bitmap_size_bytes, uint32_t *total_valid_size_out) {

    if (!device || !valid_bitmap || !total_valid_size_out) {
        return UINT32_MAX;
    }

    // Шаг 1: Определяем параметры в зависимости от режима
    const uint8_t *real_usage_bitmap;
    uint32_t page_count;
    uint32_t words_per_page;
    uint32_t word_size;
    uint32_t start_page_global_num;
    uint32_t total_words_in_area = bitmap_size_bytes * 8;

    if (clean_mod == CLEAN_DATA) {

        if (bitmap_size_bytes != device->superblock.bitmap_size_bytes)
            return UINT32_MAX;

        real_usage_bitmap     = device->bitmap;
        page_count            = device->superblock.userdata_page_count;
        words_per_page        = device->superblock.words_per_page;
        word_size             = device->superblock.word_size_bytes;
        start_page_global_num = device->superblock.data_offset / device->superblock.page_size_bytes;

    } else if (clean_mod == CLEAN_METADATA) {

        if (bitmap_size_bytes != device->superblock.metadata_bitmap_size_bytes)
            return UINT32_MAX;

        real_usage_bitmap     = device->metadata_bitmap;
        uint32_t page_size    = device->superblock.page_size_bytes;
        word_size             = sizeof(kvs_metadata);
        words_per_page        = page_size / word_size;
        page_count            = (device->superblock.metadata_size_bytes + page_size - 1) / page_size;
        start_page_global_num = device->superblock.metadata_offset / page_size;

    } else {
        return UINT32_MAX;
    }

    // Шаг 2: Ищем максимально грязную страницу, проходясь по всем слова страницы
    // ищем слова, которые будут считаться невалидными и также не будут пустыми
    uint32_t victim_page = UINT32_MAX;
    uint32_t max_garbage_words = 0;
    *total_valid_size_out = 0;

    for (uint32_t p = 0; p < page_count; p++) {
        uint32_t garbage_words_on_page = 0;
        uint32_t valid_words_on_page = 0;
        uint32_t start_word_index = p * words_per_page;

        for (uint32_t w = 0; w < words_per_page; w++) {
            uint32_t current_word_index = start_word_index + w;
            if (current_word_index >= total_words_in_area) break;

            bool is_used_in_reality    = get_bit(real_usage_bitmap, current_word_index);
            bool is_used_by_valid_data = get_bit(valid_bitmap, current_word_index);

            // Мусор - это то, что помечено как используемое, но не является валидным.
            if (is_used_in_reality && !is_used_by_valid_data) {
                garbage_words_on_page++;
            } else if (is_used_by_valid_data) {
                valid_words_on_page++;
            }
        }

        // Обновляем данные, если нашли более "грязную" страницу
        if (garbage_words_on_page > max_garbage_words) {
            max_garbage_words = garbage_words_on_page;
            victim_page = p + start_page_global_num;
            *total_valid_size_out = valid_words_on_page * word_size;
        }
    }

    if (max_garbage_words == 0) {
        // Мусор не найден
        return UINT32_MAX;
    }

    return victim_page;
}

uint32_t kvs_gc(int clean_mod){

    if(!device)
        return 0;

    if (clean_mod == CLEAN_DATA){

        // Фаза 1: Анализ и поиск страницы-жертвы
        uint32_t bitmap_size = device->superblock.bitmap_size_bytes;
        uint8_t *valid_bitmap = calloc(1,bitmap_size);
        if (!valid_bitmap) {
            kvs_log("GC Ошибка: не удалось выделить память для valid_bitmap.");
            return 0;
        }

        // Заполняем биткарту, в которой: 1 - слово валидно, 0 - слово невалидно
        for( int i = 0; i < device->key_count; i++ ) {

            if(is_key_valid(i) == 1){

                kvs_metadata temp;
                if(kvs_read_region(device->fp, device->key_index[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                    continue;

                uint32_t word_size  = device->superblock.word_size_bytes;
                uint32_t start_word = (temp.value_offset - device->superblock.data_offset) / word_size;
                uint32_t num_words  = align_up(temp.value_size, word_size) / word_size;

                for (uint32_t k = 0; k < num_words; k++) {
                    uint32_t current_word_index = start_word + k;
                    if ((current_word_index / 8) >= bitmap_size) continue;
                    uint32_t byte_index = current_word_index / 8;
                    uint8_t  bit_index  = current_word_index % 8;
                    valid_bitmap[byte_index] |= (1 << bit_index);
                }

            }

        }

        // Находим страницу в которой максимальное количество невалидных данных
        // live_data_on_page - размер валидных данных на странице
        uint32_t live_data_on_page = 0;
        uint32_t victim_page = kvs_find_victim_page(CLEAN_DATA, valid_bitmap, bitmap_size, &live_data_on_page);

        free(valid_bitmap);

        if(victim_page == UINT32_MAX){
            kvs_log("GC: Не найдено подходящих для очистки страниц данных.");
            return 0;
        }

        uint32_t victim_page_start = victim_page * device->superblock.page_size_bytes;

        if(live_data_on_page == 0){
            kvs_log("GC: Страница #%u не содержит живых данных. Просто стираем.", victim_page);
            if( ssdmmc_sim_erase_page(device->fp, victim_page) < 0 )
                return 0;

            rewrite_count_increment_region(victim_page_start, device->superblock.page_size_bytes);
            bitmap_clear_region(victim_page_start, device->superblock.page_size_bytes);
            crc_update_region(victim_page_start, device->superblock.page_size_bytes);

            return kvs_persist_all_service_data() == KVS_INTERNAL_OK ? device->superblock.page_size_bytes : 0;
        }

        // Фаза 2: Подготовка к эвакуации
        uint32_t new_base_offset = kvs_find_free_data_offset(live_data_on_page);
        if(new_base_offset == UINT32_MAX){
            kvs_log("GC Ошибка: нет места для эвакуации %u байт живых данных.", live_data_on_page);
            return 0;
        }

        gc_item  *items_to_move = calloc(device->key_count,sizeof(gc_item));
        uint32_t items_count = 0;
        uint32_t buffer_offset_counter = 0;
        uint32_t victim_page_end = victim_page_start + device->superblock.page_size_bytes;

        for( int i = 0; i < device->key_count; i++){

            if(is_key_valid(i) != 1)
                continue;

            // Считываем метаданные, в которых есть невалидные данные
            kvs_metadata temp;
            if(kvs_read_region(device->fp, device->key_index[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0) continue;

            // Если данные находятся в странице, которую мы собрались очищать,
            // то добавляем их в массив эвакуации
            if(temp.value_offset >= victim_page_start && temp.value_offset < victim_page_end){
                items_to_move[items_count].metadata_offset    = device->key_index[i].metadata_offset;
                items_to_move[items_count].old_value_offset   = temp.value_offset;
                items_to_move[items_count].aligned_value_size = align_up(temp.value_size, device->superblock.word_size_bytes);
                items_to_move[items_count].offset_in_buffer   = buffer_offset_counter;

                buffer_offset_counter += items_to_move[items_count].aligned_value_size;
                items_count++;
            }

        }

        // Фаза 3: Выполнение эвакуации
        if (items_count > 0) {
            uint8_t *evacuation_buffer = calloc(1,live_data_on_page);
            if (!evacuation_buffer)
                return 0;

            // Считываем в буфер спасаемые данные со страницы
            for (uint32_t i = 0; i < items_count; i++) {
                kvs_read_region(device->fp, items_to_move[i].old_value_offset, evacuation_buffer + items_to_move[i].offset_in_buffer, items_to_move[i].aligned_value_size);
            }

            // Записываем спасенные данные в новое место
            if (kvs_write_region(device->fp, new_base_offset, evacuation_buffer, live_data_on_page) < 0) {
                free(evacuation_buffer);
                return 0;
            }

            // Очищаем страницу, с которой спасли данные
            if(ssdmmc_sim_erase_page(device->fp, victim_page) < 0) {
                free(evacuation_buffer);
                return 0;
            }

            free(evacuation_buffer);

            // Фаза 4: Обновление всех необходимых служебных данных
            for (uint32_t i = 0; i < items_count; i++) {
                kvs_metadata temp;
                if(kvs_read_region(device->fp, items_to_move[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                    continue;
                temp.value_offset = new_base_offset + items_to_move[i].offset_in_buffer;
                if(kvs_write_region(device->fp, items_to_move[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                    continue;
                uint32_t slot_index = (items_to_move[i].metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
                kvs_update_single_metadata_crc(slot_index);
            }
        }

        bitmap_clear_region(victim_page_start, device->superblock.page_size_bytes);
        bitmap_set_region(new_base_offset, live_data_on_page);
        rewrite_count_increment_region(victim_page_start, device->superblock.page_size_bytes);
        rewrite_count_increment_region(new_base_offset, live_data_on_page);
        crc_update_region(new_base_offset, live_data_on_page);

        if(kvs_persist_all_service_data() != KVS_INTERNAL_OK)
            return 0;

        kvs_log("GC: Сборка мусора для данных успешно завершена.");
        return device->superblock.page_size_bytes;

    }

    else if (clean_mod == CLEAN_METADATA){

        // Фаза 1: Анализ и поиск страницы-жертвы для метаданных
        uint32_t metadata_bitmap_size = device->superblock.metadata_bitmap_size_bytes;
        uint8_t *valid_metadata_bitmap = calloc(1,metadata_bitmap_size);
        if (!valid_metadata_bitmap) {
            kvs_log("GC (Метаданные) Ошибка: не удалось выделить память для valid_bitmap.");
            return 0;
        }

        // Создаем временную карту валидных слотов на основе текущего key_index
        for (uint32_t i = 0; i < device->key_count; i++) {
            if (is_key_valid(i) == 1) {
                uint32_t slot_index = (device->key_index[i].metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
                uint32_t byte_index = slot_index / 8;
                uint8_t  bit_index  = slot_index % 8;
                if (byte_index < metadata_bitmap_size) {
                    valid_metadata_bitmap[byte_index] |= (1 << bit_index);
                }
            }
        }

        uint32_t live_metadata_on_page = 0;
        uint32_t victim_page = kvs_find_victim_page(CLEAN_METADATA, valid_metadata_bitmap, metadata_bitmap_size, &live_metadata_on_page);

        if (victim_page == UINT32_MAX) {
            free(valid_metadata_bitmap);
            kvs_log("GC (Метаданные): Не найдено подходящих для очистки страниц метаданных.");
            return 0;
        }

        uint32_t victim_page_start = victim_page * device->superblock.page_size_bytes;

        // Если на странице нет "живых" данных, просто стираем ее и пересобираем биткарты
        if (live_metadata_on_page == 0) {
            if (ssdmmc_sim_erase_page(device->fp, victim_page) < 0) {
                free(valid_metadata_bitmap);
                return 0;
            }
        } else {
            // Фаза 2: Эвакуация "живых" метаданных со страницы-жертвы
            uint8_t *evacuation_buffer = calloc(1,live_metadata_on_page);
            if(!evacuation_buffer) {
                free(valid_metadata_bitmap);
                return 0;
            }

            uint32_t buffer_offset_counter = 0;
            uint32_t slots_per_page = device->superblock.page_size_bytes / sizeof(kvs_metadata);
            uint32_t start_slot = (victim_page_start - device->superblock.metadata_offset) / sizeof(kvs_metadata);

            for (uint32_t i = 0; i < slots_per_page; i++) {
                uint32_t current_slot = start_slot + i;
                if (get_bit(valid_metadata_bitmap, current_slot) == 1) {
                    uint32_t old_offset = device->superblock.metadata_offset + (current_slot * sizeof(kvs_metadata));
                    kvs_read_region(device->fp, old_offset, evacuation_buffer + buffer_offset_counter, sizeof(kvs_metadata));
                    buffer_offset_counter += sizeof(kvs_metadata);
                }
            }

            // Стираем старую страницу после того, как скопировали из нее все живое
            if (ssdmmc_sim_erase_page(device->fp, victim_page) < 0) {
                free(evacuation_buffer);
                free(valid_metadata_bitmap);
                return 0;
            }

            // Записываем спасенные метаданные в новые свободные места
            for (uint32_t i = 0; i < (live_metadata_on_page / sizeof(kvs_metadata)); i++) {
                uint32_t new_meta_offset = kvs_find_free_metadata_offset();
                if (new_meta_offset == UINT32_MAX) {
                    kvs_log("GC (Метаданные) КРИТИЧЕСКАЯ ОШИБКА: нет места для эвакуации.");
                    free(evacuation_buffer);
                    free(valid_metadata_bitmap);
                    return 0;
                }
                kvs_write_region(device->fp, new_meta_offset, evacuation_buffer + (i * sizeof(kvs_metadata)), sizeof(kvs_metadata));
            }
            free(evacuation_buffer);
        }

        free(valid_metadata_bitmap);

        // Фаза 3: Полный пересбор служебных структур
        kvs_log("GC (Метаданные): Запускаем полный пересбор служебных структур...");

        // Перестраиваем биткарту метаданных с диска
        if (kvs_metadata_bitmap_create() != KVS_INTERNAL_OK) return 0;

        // Перестраиваем key_index, отбрасывая "мертвые" ссылки
        if (build_key_index() != KVS_INTERNAL_OK) return 0;

        // Перестраиваем биткарту данных.
        if (kvs_bitmap_create() != KVS_INTERNAL_OK) return 0;

        // Сохраняем все обновленные структуры на диск
        if (kvs_persist_all_service_data() != KVS_INTERNAL_OK) return 0;

        kvs_log("GC: Сборка мусора для метаданных и пересбор структур успешно завершены.");
        return device->superblock.page_size_bytes;

    }

    else {
        kvs_log("Ошибка GC: Указан неверный режим очистки данных");
    }

    return 0;
}

kvs_internal_status kvs_verify_and_prepare_region(uint32_t offset, uint32_t size)
{
    // Шаг 1: Проверяем базовые условия и определяем, с какой областью работаем
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (size == 0) {
        return KVS_INTERNAL_OK;
    }

    uint8_t *bitmap;
    uint32_t word_size;
    uint32_t area_start_offset;

    // Определяем параметры в зависимости от того, где находится регион
    if (offset >= device->superblock.data_offset && (offset + size) <= device->superblock.metadata_offset) {
        // Работаем с областью пользовательских данных
        bitmap = device->bitmap;
        word_size = device->superblock.word_size_bytes;
        area_start_offset = device->superblock.data_offset;
    } else if (offset >= device->superblock.metadata_offset && (offset + size) <= device->superblock.superblock_backup_offset) {
        // Работаем с областью метаданных
        bitmap = device->metadata_bitmap;
        word_size = sizeof(kvs_metadata);
        area_start_offset = device->superblock.metadata_offset;
    } else {
        // Указан некорректный регион
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Шаг 2: Итерируемся по всем страницам, затронутым регионом
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t first_page = offset / page_size;
    uint32_t last_page = (offset + size - 1) / page_size;

    for (uint32_t p = first_page; p <= last_page; p++) {
        uint32_t current_page_offset = p * page_size;
        bool discrepancy_found = false;

        // Шаг 3: Предварительная проверка страницы на несоответствия
        for (uint32_t w_offset = 0; w_offset < page_size; w_offset += word_size) {
            uint32_t current_word_abs_offset = current_page_offset + w_offset;

            // Пропускаем слова, которые находятся за пределами запрошенного региона
            if (current_word_abs_offset < offset || current_word_abs_offset >= (offset + size)) {
                continue;
            }

            uint32_t word_index = (current_word_abs_offset - area_start_offset) / word_size;

            if (get_bit(bitmap, word_index) == 0) {
                // Биткарта говорит, что слово свободно. Проверяем, так ли это на самом деле.
                if (is_data_region_empty(current_word_abs_offset, word_size) == 0) {
                    // Найдено несоответствие. Бит свободен, но место занято мусором.
                    // Дальше эту страницу проверять нет смысла, переходим к очистке.
                    discrepancy_found = true;
                    break;
                }
            }
        }

        // Шаг 4: Если найдено несоответствие, выполняем принудительную очистку
        if (discrepancy_found) {
            // Выделяем буфер под всю страницу
            uint8_t *page_buffer = calloc(1, page_size);
            if (!page_buffer) {
                return KVS_INTERNAL_ERR_MALLOC_FAILED;
            }

            // Считываем всю страницу в буфер
            if (kvs_read_region(device->fp, current_page_offset, page_buffer, page_size) < 0) {
                free(page_buffer);
                return KVS_INTERNAL_ERR_READ_FAILED;
            }

            // Проходим по буферу и очищаем все места, которые должны быть пустыми
            for (uint32_t w_offset = 0; w_offset < page_size; w_offset += word_size) {
                uint32_t current_word_abs_offset = current_page_offset + w_offset;
                uint32_t word_index = (current_word_abs_offset - area_start_offset) / word_size;

                if (get_bit(bitmap, word_index) == 0) {
                    // Этот участок должен быть пуст, затираем его в буфере
                    memset(page_buffer + w_offset, 0xFF, word_size);
                }
            }

            // Стираем физическую страницу
            if (ssdmmc_sim_erase_page(device->fp, p) < 0) {
                free(page_buffer);
                return KVS_INTERNAL_ERR_ERASE_FAILED;
            }

            // Записываем измененный (очищенный) буфер обратно
            if (kvs_write_region(device->fp, current_page_offset, page_buffer, page_size) < 0) {
                free(page_buffer);
                return KVS_INTERNAL_ERR_WRITE_FAILED;
            }

            free(page_buffer);
        }
    }

    return KVS_INTERNAL_OK;
}
