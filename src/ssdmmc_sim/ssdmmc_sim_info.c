#include "ssdmmc_sim_internal.h"

uint32_t ssdmmc_sim_get_page_count(void){
    return SSDMMC_SIM_PAGE_COUNT;
}

uint32_t ssdmmc_sim_get_words_per_page(void){
    return SSDMMC_SIM_WORDS_PER_PAGE;
}

uint32_t ssdmmc_sim_get_word_size(void){
    return SSDMMC_SIM_WORD_SIZE;
}