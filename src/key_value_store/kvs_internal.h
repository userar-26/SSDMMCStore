#ifndef SSDMMCSTORE_KVS_INTERNAL_H
#define SSDMMCSTORE_KVS_INTERNAL_H


#include "kvs_types.h"


// Освобождает всю память, выделенную под устройство, и закрывает файл.
void kvs_free_device();

// Записывает сообщение в лог-файл библиотеки KVS.
// Сообщение добавляется в конец файла лога. Формат сообщения аналогичен printf.
// Если файл лога не удается открыть, функция ничего не делает.
void kvs_log(const char *format, ...);

// Создает директорию, в которой будут храниться все файлы с данными, если ее еще нет.
void kvs_make_data_dir(void);

// Выравнивает значение size вверх до ближайшего кратного align.
// Например, align_up(13, 8) вернет 16.
// size - исходное значение
// align - кратность
// Возвращает выровненное вверх значение.
uint32_t align_up(uint32_t size, uint32_t align);


#endif
