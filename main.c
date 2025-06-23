#include "kvs.h"

#define STORAGE_SIZE (1024 * 24)

int main() {

    char key[KVS_KEY_SIZE];
    snprintf(key,KVS_KEY_SIZE,"%s","key");
    char data[1024],data2[1024];
    snprintf(data,1024,"%s","data");

    kvs_init(STORAGE_SIZE);

    kvs_put(key,KVS_KEY_SIZE,data,1024);

    if(kvs_exists(key) == 1){
        size_t size = 1024;
        kvs_get(key,data2,&size);
        printf("Ключ: %s с данными %s размером %lu - существует\n\n",key,data2,size);
    }

    kvs_deinit();

    kvs_init(STORAGE_SIZE);

    if(kvs_exists(key) == 1){
        size_t size = 1024;
        kvs_get(key,data2,&size);
        printf("Ключ: %s с данными %s размером %lu - существует\n\n",key,data2,size);
    }

    kvs_delete(key);

    kvs_deinit();

    kvs_init(STORAGE_SIZE);

    if(kvs_exists(key) == 1){
        size_t size = 1024;
        kvs_get(key,data2,&size);
        printf("Ключ: %s с данными %s размером %lu - существует\n\n",key,data2,size);
    }
    else{
        printf("Ключ: %s - не существует\n",key);
    }

    kvs_deinit();
}
