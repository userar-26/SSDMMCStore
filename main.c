#include "kvs.h"
#include "unistd.h"

#define NUM_KEYS 10
#define MAX_SIZE 1024
#define STORAGE_SIZE (1024 * 24 * 2)

int main() {

    unlink("../data/kvs_storage.bin");

    srand(time(NULL));
    kvs_init(STORAGE_SIZE);

    char data_buf[NUM_KEYS][MAX_SIZE];
    size_t  rand_size[NUM_KEYS];
    char key[NUM_KEYS][KVS_KEY_SIZE];
    char **rand_data_buf = NULL;

    for(int i=0;i<NUM_KEYS;i++){
        snprintf(key[i],KVS_KEY_SIZE,"key%d",i);
    }
    for(int i=0;i<NUM_KEYS;i++){
        rand_size[i]=rand()%(1024-200)+200;
    }

    rand_data_buf = calloc(NUM_KEYS,sizeof(char *));
    if(!rand_data_buf)
        return 1;

    for(int i=0;i<NUM_KEYS;i++){
        rand_data_buf[i] = calloc(1,rand_size[i]);
        if(!rand_data_buf[i]){
            free(rand_data_buf);
            return 1;
        }
        snprintf(rand_data_buf[i],rand_size[i],"data%d",i);
    }

    printf("\nПроверяем работоспособность kvs_put\n\n");
    for(int i=0;i<NUM_KEYS;i++){
        if(kvs_put(key[i],KVS_KEY_SIZE,rand_data_buf[i],rand_size[i]) == 0){
            printf("Ключ: %5s с данными %10s, размером %5lu был добавлен в хранилище\n", key[i],rand_data_buf[i],rand_size[i]);
        }
    }
    printf("\nПроверяем существование записанных данных до деинициализации\n\n");
    for(int i=0;i<NUM_KEYS;i++){
        if(kvs_exists(key[i])){
            if(kvs_get(key[i],data_buf[i],&rand_size[i]) == 0){
                printf("Ключ: %5s с данными %10s, размером %5lu был считан из хранилища\n", key[i],rand_data_buf[i],rand_size[i]);
            }
        }
    }

    printf("\nПеред деинициализацией удаляем ключ %s\n\n",key[0]);
    kvs_delete(key[0]);

    kvs_deinit();
    kvs_init(STORAGE_SIZE);

    printf("После деинициализации проверяем данные\n\n");

    for(int i=0;i<NUM_KEYS;i++){
        if(kvs_exists(key[i])){
            if(kvs_get(key[i],data_buf[i],&rand_size[i]) == 0){
                printf("Ключ: %5s с данными %10s, размером %5lu был считан из хранилища\n", key[i],rand_data_buf[i],rand_size[i]);
            }
        }
    }

    kvs_deinit();

}
