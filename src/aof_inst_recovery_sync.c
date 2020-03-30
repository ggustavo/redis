#include "server.h"

#include <stdio.h>
#include <stdlib.h>

#include <sys/file.h>  // To Open ?
#include <sys/types.h> // To Read and Write
#include <unistd.h>    // To Close and To Sleep
#include <fcntl.h>
#include <sys/stat.h>






struct Command * parserAOFCommand(size_t CHUNK_SIZE, char * BUFFER, size_t offset){
    int current_state = STATE_1;
    
    char ** tokens = NULL;
    int * tokens_size = NULL;

    int number_of_sentences = 0;
    int current_sentence_index = 0;
    int current_sentence_size = 0;
    char * current_sentence = NULL;

    for(size_t i = offset; i < CHUNK_SIZE; i++){
        if(i == CHUNK_SIZE-1 && current_state == STATE_6){
        }else if( IS_LINE_BREAK(BUFFER[i])){
             continue; 
        }

        if(current_state == STATE_1){
            current_state = BUFFER[i] == '*' ? STATE_2 : STATE_ERROR;
            
          // printf("\nState 1: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_2){  
            number_of_sentences = read_positive_int(CHUNK_SIZE, BUFFER, &i);  
            if(number_of_sentences > 0){
                tokens = (char **)   zmalloc( sizeof(char*) * number_of_sentences); 
                tokens_size = (int*) zmalloc( sizeof(int) * number_of_sentences);
                
                for(int h_t = 0; h_t < number_of_sentences; h_t++){
                    tokens[h_t] = NULL;
                    tokens_size[h_t] = NULL;
                }
                current_state = STATE_3;
            }else{
                current_state = STATE_ERROR;
            }

           // printf("\nState 2: BUFFER[%d]: %c -> number_of_sentences: %d ", i, BUFFER[i], number_of_sentences );
        }else

        if(current_state == STATE_3){
            current_state = BUFFER[i] == '$' ? STATE_4 : STATE_ERROR;
            
           // printf("\nState 3: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_4){
            current_sentence_size = read_positive_int(CHUNK_SIZE, BUFFER, &i);
            current_state = current_sentence_size > 0 ? STATE_5 : STATE_ERROR;
            
           // printf("\nState 4: BUFFER[%d]: %c -> current_sentence_size: %d ", i, BUFFER[i], current_sentence_size );
        }else

        if(current_state == STATE_5){
            current_sentence = read_string(CHUNK_SIZE, BUFFER, &i, current_sentence_size);
            if(current_sentence != NULL){
                tokens[current_sentence_index] = current_sentence;
                tokens_size[current_sentence_index] = current_sentence_size;
                current_sentence_index++;
                //zfree(current_sentence); //<----- remove this, to use tokens
                current_state = STATE_6;
            }else{
                current_state = STATE_ERROR;
            }

           // printf("\nState 5: BUFFER[%d]: %c -> current_sentence: %s ", i, BUFFER[i], current_sentence );
        }else

        if(current_state == STATE_6){
            if(current_sentence_index == number_of_sentences){
                struct Command * command = (struct Command *) zmalloc(sizeof(struct Command));
                command->number_of_tokens = number_of_sentences;
                command->tokens = tokens;
                command->tokens_size = tokens_size;
                command->size = i - offset;
                command->log_offset_start = 0;
                command->log_offset_end = 0;
                return command; // FINAL STATE
            }
            current_state = BUFFER[i] == '$' ? STATE_4 : STATE_ERROR;

           // printf("\nState 6: BUFFER[%d]: %c -> %d sentences remain ", i, BUFFER[i], number_of_sentences - current_sentence_index );
        }else

        if(current_state == STATE_ERROR){
            print_parse_error(CHUNK_SIZE, BUFFER, i);
            break;
        }

    }
    
    free_tokens(tokens,tokens_size, number_of_sentences);
   
    return NULL;
}

void free_tokens(char ** tokens, int* tokens_size, int number_of_sentences){
    if(tokens != NULL){
        for(int index = 0; index < number_of_sentences; index++){
            if(tokens[index] != NULL){
                zfree(tokens[index]);
                //zfree(tokens_size[index]);
            }
            tokens[index] = NULL;
            //tokens_size[index] = NULL;
        }
        zfree(tokens);
        zfree(tokens_size);
    }
}

char * read_string(size_t CHUNK_SIZE, char * BUFFER, size_t * i, int size){
    if(*i + size >= CHUNK_SIZE) return NULL;
    char * string = (char*) zmalloc(sizeof(size + 1));
    string[size] = '\0';
    
    int k = 0;
    while(k < size){
        string[k] = BUFFER[*i];
        *i = *i + 1;
        k = k + 1;
    }
    return string;
}

int read_positive_int(size_t CHUNK_SIZE, char * BUFFER, size_t * i){
    int num = 0;

    while ( *i < CHUNK_SIZE && BUFFER[*i] && ( BUFFER[*i] >= '0' && BUFFER[*i] <= '9') ){
        num = num * 10 + (BUFFER[*i] - '0');
        *i = *i + 1;
        
    }
    if( *i < CHUNK_SIZE && IS_LINE_BREAK(BUFFER[*i])){
        return num;
    }
    return -1;
}


void print_parse_error(size_t CHUNK_SIZE, char * BUFFER, size_t offset){
    size_t print_parse_error_msg_size = 30;
    printf("\nParse ERROR at BUFFER[%ld], start -> ", offset);
    for (size_t i = offset; i < MIN(offset + print_parse_error_msg_size, CHUNK_SIZE); i++){
       printf("%c", BUFFER[i]);
    }
    printf("...\n");
    //exit(EXIT_FAILURE);
}

void print_raw_chunk(size_t CHUNK_SIZE, char * BUFFER){

    printf("\n---------------------------- CHUNK --------------------------\n");
    for(size_t i=0; i < CHUNK_SIZE;i++){
        printf("%c",BUFFER[i]);
    }
    printf("\n-------------------------------------------------------------\n");
}

void print_aov_command(struct Command * command){
    printf("\n[ ");
    for(int i = 0; i < command->number_of_tokens; i++){
        if(command->tokens[i] != NULL){
            printf("%s (%d) ",command->tokens[i], command->tokens_size[i]);
        }
    }
    printf("] size: %d, start: %ld, end: %ld", command->size, command->log_offset_start, command->log_offset_end);
    
}

size_t ASSERT_FILE(size_t value){
    if(value == (size_t)-1){
        perror("Redis AOF file is corrupted? ERROR");
    }
    return value;
}



int DEBUG_CYCLE = 0;

size_t run_cycle(int file, size_t file_size, char * BUFFER, size_t CHUNK_SIZE, size_t LAST_END_FILE, void (*receiver)(struct Command*)){
    
    if(file_size == 0 || LAST_END_FILE == file_size - 1) return file_size;
    
    if(file_size - LAST_END_FILE < CHUNK_SIZE){
        CHUNK_SIZE = file_size - LAST_END_FILE;
    }
    
    int interactions = (file_size - LAST_END_FILE) / CHUNK_SIZE;
    
    if(interactions == 0){
       printf("\nERROR: interactions == 0");
    }
    
    if(DEBUG_CYCLE) printf("\n\n----------------- Running Cycle (fd:%d) -------------------", file);
    if(DEBUG_CYCLE) printf("\n AOF_FILE_CURRENT_SIZE:  %ld bytes", file_size);
    if(DEBUG_CYCLE) printf("\n LAST_END_FILE:          %ld bytes", LAST_END_FILE);
    if(DEBUG_CYCLE) printf("\n CHUNK_SIZE:             %ld bytes", CHUNK_SIZE);
    if(DEBUG_CYCLE) printf("\n NUMBER_OF_INTERACTIONS: %d",       interactions);
  
    size_t seek = 0;
    size_t bytes_read = 0;
    size_t current_offset = 0;

    for (int i = 0; i < interactions; i++) {    
        seek = lseek(file, LAST_END_FILE, SEEK_SET);
        ASSERT_FILE(seek);
        
        bytes_read = read(file, BUFFER, CHUNK_SIZE);
        ASSERT_FILE(bytes_read);
        if(bytes_read != CHUNK_SIZE){
            printf("\nERROR: failed to read a chuck-block %ld/%ld", bytes_read, CHUNK_SIZE);
            exit(EXIT_FAILURE);
        }
        
        //print_raw_chunk();
        current_offset = LAST_END_FILE;

        while(1){ // While we can parse commands

            struct Command * command = parserAOFCommand(CHUNK_SIZE, BUFFER, current_offset - LAST_END_FILE);
            if(command != NULL){
                command->log_offset_start = current_offset;
                command->log_offset_end = current_offset + command->size - 1; 
                current_offset  = current_offset + command->size; // Next OFFSET!
               // print_aov_command(command);
               // printf("\n");
               // fflush(stdout);
                receiver(command);
            }else{
                LAST_END_FILE = current_offset;
                if(DEBUG_CYCLE) printf("\nERROR: Failed to parse the last command. Opening a new chunk from the byte %ld", LAST_END_FILE);
                break;
            }

        }
    }   
    return LAST_END_FILE;
}




