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

kvs_internal_status kvs_update_entry_crc(uint32_t slot_index) {

    // Шаг 1: Проверяем базовые параметры
    if (!device || slot_index >= device->superblock.max_key_count) {
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    }

    // Шаг 2: Читаем с диска метаданные, соответствующие слоту
    kvs_metadata metadata;
    uint32_t metadata_offset = device->superblock.metadata_offset + (slot_index * sizeof(kvs_metadata));
    if (kvs_read_region(device->fp, metadata_offset, &metadata, sizeof(kvs_metadata)) < 0) {
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Шаг 3: Выделяем буфер и читаем с диска данные, на которые указывают метаданные
    uint32_t aligned_value_len = align_up(metadata.value_size, device->superblock.word_size_bytes);
    uint8_t *value_buffer = calloc(1, aligned_value_len);
    if (!value_buffer) {
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    if (kvs_read_region(device->fp, metadata.value_offset, value_buffer, aligned_value_len) < 0) {
        free(value_buffer);
        return KVS_INTERNAL_ERR_READ_FAILED;
    }

    // Шаг 4: Создаем единый буфер, объединяя метаданные и данные
    size_t total_size = sizeof(kvs_metadata) + aligned_value_len;
    uint8_t *check_buffer = calloc(1,total_size);
    if (!check_buffer) {
        free(value_buffer);
        return KVS_INTERNAL_ERR_MALLOC_FAILED;
    }
    memcpy(check_buffer, &metadata, sizeof(kvs_metadata));
    memcpy(check_buffer + sizeof(kvs_metadata), value_buffer, aligned_value_len);

    // Шаг 5: Считаем CRC для этого единого буфера
    uint32_t calculated_crc = crc32_calc(check_buffer, total_size);

    // Шаг 6: Записываем полученный CRC в соответствующую ячейку массива entry_crc в ОЗУ
    device->page_crc.entry_crc[slot_index] = calculated_crc;

    // Шаг 7: Освобождаем всю выделенную память
    free(value_buffer);
    free(check_buffer);

    return KVS_INTERNAL_OK;
}

kvs_internal_status rewrite_count_increment_region(uint32_t offset, uint32_t size)
{
    // Шаг 1: Проверяем базовые параметры
    if (!device)
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    if (offset + size > device->superblock.storage_size_bytes)
        return KVS_INTERNAL_ERR_INVALID_PARAM;
    if (size == 0)
        return KVS_INTERNAL_OK;

    // Шаг 2: Вычисляем глобальные номера первой и последней страниц, затронутых операцией
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t first_global_page = offset / page_size;
    uint32_t last_global_page  = (offset + size - 1) / page_size;

    // Шаг 3: Определяем, с какой глобальной страницы начинается отслеживаемая область (userdata + metadata)
    uint32_t start_tracked_page = device->superblock.data_offset / page_size;

    // Шаг 4: В цикле проходим по всем затронутым страницам
    for (uint32_t i = first_global_page; i <= last_global_page; i++) {
        // Увеличиваем счетчик, только если текущая страница входит в отслеживаемую зону
        if (i >= start_tracked_page) {
            uint32_t rewrite_index = i - start_tracked_page;
            device->page_rewrite_count[rewrite_index]++;
        }
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
    // Шаг 1: Проверяем, инициализировано ли устройство
    if(!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    // Шаг 2: Сначала полностью очищаем битовую карту в памяти
    memset(device->bitmap, 0, device->superblock.bitmap_size_bytes);
    if (device->key_count == 0) {
        return KVS_INTERNAL_OK;
    }
    // Шаг 3: Проходим по всем валидным ключам в key_index
    kvs_metadata temp;

    for(uint32_t i = 0; i < device->key_count; i++)
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

    // Шаг 1: Делаем базовую проверку
    if(!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    // Шаг 2: Обнуляем текущее значение ключей перед построением
    device->key_count = 0;

    // Шаг 3: Проходим по всем возможным слотам метаданных
    kvs_metadata temp;
    for (uint32_t i = 0; i < device->superblock.max_key_count; i++) {

        // Используем биткарту метаданных для быстрой проверки: если слот свободен, пропускаем его.
        if (!get_bit(device->metadata_bitmap, i)) {
            continue;
        }

        // Если бит установлен, читаем слот с диска
        uint32_t current_position = device->superblock.metadata_offset + (i * sizeof(kvs_metadata));
        if (kvs_read_region(device->fp, current_position, &temp, sizeof(temp)) < 0) {
            return KVS_INTERNAL_ERR_READ_FAILED;
        }

        // На этом этапе мы не можем проверить полный CRC, так как key_index еще не построен.
        // Мы делаем только базовую проверку на валидность смещений и размеров.
        if (is_metadata_entry_valid(&temp)) {
            kvs_add_metadata_entry(&temp, current_position);
        }
    }

    // Шаг 4: После добавления всех валидных ключей, сортируем key_index для быстрого поиска
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

kvs_internal_status kvs_metadata_bitmap_create(void) {
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    memset(device->metadata_bitmap, 0, device->superblock.metadata_bitmap_size_bytes);
    for (uint32_t i = 0; i < device->superblock.max_key_count; i++) {
        uint32_t slot_offset = device->superblock.metadata_offset + (i * sizeof(kvs_metadata));
        if (is_data_region_empty(slot_offset, sizeof(kvs_metadata)) == 0) {
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

uint32_t kvs_find_victim_page(int clean_mod, const uint8_t *valid_bitmap, uint32_t bitmap_size_bytes, uint32_t *total_valid_size_out) {

    // Шаг 1: Проверяем базовые параметры
    if (!device || !valid_bitmap || !total_valid_size_out) {
        return UINT32_MAX;
    }

    // Шаг 2: Определяем параметры в зависимости от режима (данные или метаданные)
    const uint8_t *real_usage_bitmap;
    uint32_t page_count;
    uint32_t words_per_page;
    uint32_t word_size;
    uint32_t total_words_in_area;
    uint32_t *last_checked_word_ptr;

    if (clean_mod == CLEAN_DATA) {
        if (bitmap_size_bytes != device->superblock.bitmap_size_bytes) return UINT32_MAX;
        real_usage_bitmap     = device->bitmap;
        page_count            = device->superblock.userdata_page_count;
        words_per_page        = device->superblock.words_per_page;
        word_size             = device->superblock.word_size_bytes;
        total_words_in_area   = device->superblock.userdata_size_bytes / word_size;
        last_checked_word_ptr = &device->superblock.last_data_word_checked;
    } else if (clean_mod == CLEAN_METADATA) {
        if (bitmap_size_bytes != device->superblock.metadata_bitmap_size_bytes) return UINT32_MAX;
        real_usage_bitmap     = device->metadata_bitmap;
        word_size             = sizeof(kvs_metadata);
        words_per_page        = device->superblock.page_size_bytes / word_size;
        page_count            = (device->superblock.metadata_size_bytes + device->superblock.page_size_bytes - 1) / device->superblock.page_size_bytes;
        total_words_in_area   = device->superblock.max_key_count;
        last_checked_word_ptr = &device->superblock.last_metadata_slot_checked;
    } else {
        return UINT32_MAX;
    }

    // Шаг 3: Ищем максимально "грязную" страницу, используя "карусельный" обход для выравнивания износа
    uint32_t victim_page_local = UINT32_MAX;
    uint32_t max_garbage_words = 0;
    *total_valid_size_out = 0;

    // Определяем, с какой локальной страницы начать поиск
    uint32_t start_page_local = (*last_checked_word_ptr) / words_per_page;
    if (start_page_local >= page_count) {
        start_page_local = 0;
    }

    // Проходим по всем страницам один раз по кругу
    for (uint32_t i = 0; i < page_count; i++) {
        uint32_t current_page_local = (start_page_local + i) % page_count;

        uint32_t garbage_words_on_page = 0;
        uint32_t valid_words_on_page = 0;
        uint32_t start_word_index = current_page_local * words_per_page;

        // Анализируем каждое слово на текущей локальной странице
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

        // Если текущая страница грязнее предыдущего кандидата, выбираем ее
        if (garbage_words_on_page > max_garbage_words) {
            max_garbage_words = garbage_words_on_page;
            victim_page_local = current_page_local; // Сохраняем ЛОКАЛЬНЫЙ индекс
            *total_valid_size_out = valid_words_on_page * word_size;

            // Обновляем указатель карусели, чтобы следующий поиск начался со следующей страницы
            *last_checked_word_ptr = (current_page_local + 1) * words_per_page;
            if (*last_checked_word_ptr >= total_words_in_area) {
                *last_checked_word_ptr = 0;
            }
        }
    }

    // Если мусор не найден, возвращаем ошибку
    if (max_garbage_words == 0) {
        return UINT32_MAX;
    }

    // Возвращаем ЛОКАЛЬНЫЙ индекс страницы-жертвы
    return victim_page_local;
}

uint32_t kvs_gc(int clean_mod){

    // Шаг 1: Проверяем базовые параметры
    if(!device)
        return 0;

    if (clean_mod == CLEAN_DATA){

        // Шаг 2: Анализ и поиск страницы-жертвы.
        uint32_t bitmap_size = device->superblock.bitmap_size_bytes;
        uint8_t *valid_bitmap = calloc(1,bitmap_size);
        if (!valid_bitmap) {
            kvs_log("GC Ошибка: не удалось выделить память для valid_bitmap.");
            return 0;
        }

        // Проходим по всем ключам в кеше и отмечаем их данные во временной биткарте
        for( int i = 0; i < device->key_count; i++ ) {
            if(is_key_valid(i) == 1){
                kvs_metadata temp;
                if(kvs_read_region(device->fp, device->key_index[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0) continue;

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

        // Находим локальный индекс страницы с наибольшим количеством мусора
        uint32_t live_data_on_page = 0;
        uint32_t victim_page_local = kvs_find_victim_page(CLEAN_DATA, valid_bitmap, bitmap_size, &live_data_on_page);
        free(valid_bitmap);

        if(victim_page_local == UINT32_MAX){
            kvs_log("GC: Не найдено подходящих для очистки страниц данных.");
            return 0;
        }

        // Шаг 3: Точное определение границ и проверка наложения
        uint32_t page_size = device->superblock.page_size_bytes;

        // Вычисляем абсолютное смещение начала нашей локальной страницы-жертвы.
        uint32_t victim_region_start = device->superblock.data_offset + (victim_page_local * page_size);

        // Теперь вычисляем глобальный номер страницы, в которую попадает это смещение.
        uint32_t victim_page_global = victim_region_start / page_size;

        // Абсолютное смещение начала и конца этой ГЛОБАЛЬНОЙ страницы
        uint32_t victim_page_start_global = victim_page_global * page_size;
        uint32_t victim_page_end_global = victim_page_start_global + page_size;

        // Проверяем, не пересекается ли страница-жертва с областью метаданных
        if (victim_page_end_global > device->superblock.metadata_offset) {
            for (uint32_t i = 0; i < device->superblock.max_key_count; i++) {
                uint32_t slot_offset = device->superblock.metadata_offset + (i * sizeof(kvs_metadata));
                if (slot_offset >= victim_page_start_global && slot_offset < victim_page_end_global) {
                    if (get_bit(device->metadata_bitmap, i)) {
                        kvs_log("GC: Страница #%u содержит валидные метаданные. Очистка отменена.", victim_page_global);
                        return 0;
                    }
                }
            }
        }

        // Шаг 4: Обработка страницы-жертвы
        if(live_data_on_page == 0){
            kvs_log("GC: Логическая страница #%u не содержит живых данных. Очищаем регион.", victim_page_local);

            // Используем kvs_clear_region для безопасной очистки физической области
            if( kvs_clear_region(device->fp, victim_region_start, page_size) < 0)
                return 0;
            if( bitmap_clear_region(victim_region_start, page_size) < 0 )
                return 0;

            rewrite_count_increment_region(victim_page_start_global, page_size);
            return kvs_persist_all_service_data() == KVS_INTERNAL_OK ? page_size : 0;
        }

        // Шаг 5: Подготовка к эвакуации живых данных
        uint32_t new_base_offset = kvs_find_free_data_offset(live_data_on_page);
        if(new_base_offset == UINT32_MAX){
            kvs_log("GC Ошибка: нет места для эвакуации %u байт живых данных.", live_data_on_page);
            return 0;
        }

        gc_item  *items_to_move = calloc(device->key_count,sizeof(gc_item));
        uint32_t items_count = 0;
        uint32_t buffer_offset_counter = 0;

        for( int i = 0; i < device->key_count; i++){

            if(is_key_valid(i) != 1)
                continue;

            kvs_metadata temp;
            if(kvs_read_region(device->fp, device->key_index[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                continue;

            if(temp.value_offset >= victim_page_start_global && temp.value_offset < victim_page_end_global){
                items_to_move[items_count].metadata_offset    = device->key_index[i].metadata_offset;
                items_to_move[items_count].old_value_offset   = temp.value_offset;
                items_to_move[items_count].aligned_value_size = align_up(temp.value_size, device->superblock.word_size_bytes);
                items_to_move[items_count].offset_in_buffer   = buffer_offset_counter;
                buffer_offset_counter += items_to_move[items_count].aligned_value_size;
                items_count++;
            }
        }

        // Шаг 6: Выполнение эвакуации
        if (items_count > 0) {
            uint8_t *evacuation_buffer = calloc(1,live_data_on_page);
            if (!evacuation_buffer) { free(items_to_move); return 0; }
            for (uint32_t i = 0; i < items_count; i++) {
                kvs_read_region(device->fp, items_to_move[i].old_value_offset, evacuation_buffer + items_to_move[i].offset_in_buffer, items_to_move[i].aligned_value_size);
            }
            if (kvs_write_region(device->fp, new_base_offset, evacuation_buffer, live_data_on_page) < 0) {
                free(evacuation_buffer); free(items_to_move); return 0;
            }

            if(kvs_clear_region(device->fp, victim_region_start, page_size) < 0) {
                free(evacuation_buffer); free(items_to_move); return 0;
            }
            free(evacuation_buffer);

            if( bitmap_clear_region(victim_region_start, page_size) < 0 ) {
                free(items_to_move);
                return 0;
            }

            for (uint32_t i = 0; i < items_count; i++) {
                kvs_metadata temp;
                if(kvs_read_region(device->fp, items_to_move[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                    continue;
                temp.value_offset = new_base_offset + items_to_move[i].offset_in_buffer;
                if(kvs_write_region(device->fp, items_to_move[i].metadata_offset, &temp, sizeof(kvs_metadata)) < 0)
                    continue;
                uint32_t slot_index = (items_to_move[i].metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
                kvs_update_entry_crc(slot_index);
            }
        }
        free(items_to_move);

        // Шаг 7: Финальное обновление служебных структур
        kvs_log("GC (Данные): Запускаем полный пересбор служебных структур...");
        if (kvs_metadata_bitmap_create() != KVS_INTERNAL_OK)
            return 0;
        if (build_key_index() != KVS_INTERNAL_OK)
            return 0;
        if (kvs_bitmap_create() != KVS_INTERNAL_OK)
            return 0;
        if (kvs_persist_all_service_data() != KVS_INTERNAL_OK)
            return 0;

        kvs_log("GC: Сборка мусора для данных успешно завершена.");
        return page_size;
    }

    else if (clean_mod == CLEAN_METADATA){

        // Шаг 2: Анализ и поиск страницы-жертвы для метаданных
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

        // Находим страницу метаданных с наибольшим количеством "мусора"
        uint32_t live_metadata_on_page = 0;
        uint32_t victim_page_local = kvs_find_victim_page(CLEAN_METADATA, valid_metadata_bitmap, metadata_bitmap_size, &live_metadata_on_page);
        free(valid_metadata_bitmap);

        if (victim_page_local == UINT32_MAX) {
            kvs_log("GC (Метаданные): Не найдено подходящих для очистки страниц метаданных.");
            return 0;
        }

        // Шаг 3: Точное определение границ и обработка
        uint32_t page_size = device->superblock.page_size_bytes;
        uint32_t victim_region_start = device->superblock.metadata_offset + (victim_page_local * page_size);

        // Если на странице нет "живых" метаданных, просто очищаем ее
        if (live_metadata_on_page == 0) {

            if (kvs_clear_region(device->fp, victim_region_start, page_size) < 0)
                return 0;
            rewrite_count_increment_region(victim_region_start, page_size);

        } else {

            // Шаг 4: Подготовка к эвакуации живых метаданных
            uint32_t slots_to_move_count = live_metadata_on_page / sizeof(kvs_metadata);
            kvs_metadata *evacuation_buffer = calloc(slots_to_move_count, sizeof(kvs_metadata));
            if(!evacuation_buffer) return 0;

            uint32_t buffer_idx = 0;
            uint32_t slots_per_page = page_size / sizeof(kvs_metadata);
            uint32_t start_slot = (victim_region_start - device->superblock.metadata_offset) / sizeof(kvs_metadata);

            // Считываем все живые слоты в буфер
            for (uint32_t i = 0; i < slots_per_page; i++) {
                uint32_t current_slot = start_slot + i;
                if (get_bit(device->metadata_bitmap, current_slot) == 1 && buffer_idx < slots_to_move_count) {

                    // Проверяем, что это действительно живой ключ
                    bool is_live = false;
                    for(uint32_t k = 0; k < device->key_count; k++) {
                        if(device->key_index[k].metadata_offset == (device->superblock.metadata_offset + current_slot * sizeof(kvs_metadata))) {
                            is_live = true;
                            break;
                        }
                    }
                    if(is_live) {
                        uint32_t old_offset = device->superblock.metadata_offset + (current_slot * sizeof(kvs_metadata));
                        kvs_read_region(device->fp, old_offset, &evacuation_buffer[buffer_idx], sizeof(kvs_metadata));
                        buffer_idx++;
                    }

                }
            }

            // Шаг 5: Эвакуация
            // Записываем спасенные метаданные в новые свободные слоты
            for (uint32_t i = 0; i < slots_to_move_count; i++) {
                uint32_t new_meta_offset = kvs_find_free_metadata_offset();
                if (new_meta_offset == UINT32_MAX) {
                    kvs_log("GC (Метаданные) КРИТИЧЕСКАЯ ОШИБКА: нет места для эвакуации.");
                    free(evacuation_buffer);
                    return 0;
                }
                kvs_write_region(device->fp, new_meta_offset, &evacuation_buffer[i], sizeof(kvs_metadata));
            }


            // Очищаем старую область
            if (kvs_clear_region(device->fp, victim_region_start, page_size) < 0) {
                free(evacuation_buffer);
                return 0;
            }
            rewrite_count_increment_region(victim_region_start, page_size);
            free(evacuation_buffer);
        }

        // Шаг 6: Финальный пересбор всех служебных структур
        kvs_log("GC (Метаданные): Запускаем полный пересбор служебных структур...");
        if (kvs_metadata_bitmap_create() != KVS_INTERNAL_OK)
            return 0;
        if (build_key_index() != KVS_INTERNAL_OK)
            return 0;
        if (kvs_bitmap_create() != KVS_INTERNAL_OK)
            return 0;

        // После пересбора key_index нужно пересчитать все CRC для записей
        for(uint32_t i = 0; i < device->key_count; i++) {
            uint32_t slot_idx = (device->key_index[i].metadata_offset - device->superblock.metadata_offset) / sizeof(kvs_metadata);
            kvs_update_entry_crc(slot_idx);
        }

        if (kvs_persist_all_service_data() != KVS_INTERNAL_OK)
            return 0;

        kvs_log("GC: Сборка мусора для метаданных и пересбор структур успешно завершены.");
        return page_size;
    }

    else
        kvs_log("GC Ошибка: Неверный режим для сборки мусора.");

    return 0;
}

kvs_internal_status kvs_verify_and_prepare_region(uint32_t offset, uint32_t size)
{
    // Шаг 1: Проверяем базовые условия
    if (!device) {
        return KVS_INTERNAL_ERR_NULL_DEVICE;
    }
    if (size == 0) {
        return KVS_INTERNAL_OK;
    }

    // Определяем параметры для работы с областью пользовательских данных
    uint8_t *bitmap = device->bitmap;
    uint32_t word_size = device->superblock.word_size_bytes;
    uint32_t page_size = device->superblock.page_size_bytes;
    uint32_t area_start_offset = device->superblock.data_offset;

    // Шаг 2: Вычисляем диапазон логических страниц, которые затрагивает наш регион
    uint32_t relative_start = offset - area_start_offset;
    uint32_t relative_end = relative_start + size;
    uint32_t first_logical_page = relative_start / page_size;
    uint32_t last_logical_page = (relative_end - 1) / page_size;

    // Шаг 3: Итерируемся по каждой логической странице
    for (uint32_t p_idx = first_logical_page; p_idx <= last_logical_page; p_idx++) {

        // Определяем абсолютные смещения для текущей логической страницы
        uint32_t logical_page_start_offset = area_start_offset + (p_idx * page_size);
        bool discrepancy_found = false;

        // Шаг 3.1: Проверяем, есть ли на этой странице мусор
        for (uint32_t w_offset = 0; w_offset < page_size; w_offset += word_size) {

            uint32_t current_word_abs_offset = logical_page_start_offset + w_offset;

            if (current_word_abs_offset >= (area_start_offset + device->superblock.userdata_size_bytes)) {
                continue;
            }

            uint32_t word_index = (current_word_abs_offset - area_start_offset) / word_size;

            if (get_bit(bitmap, word_index) == 0) {
                if (is_data_region_empty(current_word_abs_offset, word_size) == 0) {
                    discrepancy_found = true;
                    break;
                }
            }
        }

        // Шаг 3.2: Если мусор найден, выполняем безопасную очистку с сохранением валидных данных.
        if (discrepancy_found) {

            kvs_log("Обнаружен мусор на логической странице #%u. Запускаем безопасную очистку.", p_idx);

            // 1. Считываем всю логическую страницу в ОЗУ
            uint8_t *page_buffer = calloc(1, page_size);
            if (!page_buffer) return KVS_INTERNAL_ERR_MALLOC_FAILED;

            if (kvs_read_region(device->fp, logical_page_start_offset, page_buffer, page_size) < 0) {
                free(page_buffer);
                return KVS_INTERNAL_ERR_READ_FAILED;
            }

            // 2. Очищаем в буфере только те места, которые в битовой карте помечены как пустые
            for (uint32_t w_offset = 0; w_offset < page_size; w_offset += word_size) {
                uint32_t current_word_abs_offset = logical_page_start_offset + w_offset;
                if (current_word_abs_offset >= (area_start_offset + device->superblock.userdata_size_bytes)) {
                    continue;
                }
                uint32_t word_index = (current_word_abs_offset - area_start_offset) / word_size;
                if (get_bit(bitmap, word_index) == 0) {
                    memset(page_buffer + w_offset, 0xFF, word_size);
                }
            }

            // 3. Используем kvs_clear_region для безопасной физической очистки региона на диске
            if (kvs_clear_region(device->fp, logical_page_start_offset, page_size) < 0) {
                kvs_log("КРИТИЧЕСКАЯ ОШИБКА: kvs_clear_region не удалось очистить страницу.");
                free(page_buffer);
                return KVS_INTERNAL_ERR_ERASE_FAILED;
            }

            // 4. Записываем наш исправленный буфер обратно в только что очищенный регион
            if (kvs_write_region(device->fp, logical_page_start_offset, page_buffer, page_size) < 0) {
                free(page_buffer);
                return KVS_INTERNAL_ERR_WRITE_FAILED;
            }

            free(page_buffer);
        }
    }

    return KVS_INTERNAL_OK;
}