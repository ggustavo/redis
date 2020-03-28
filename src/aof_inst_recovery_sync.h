#ifndef __AOF_INST_REC_SYNC_H
#define __AOF_INST_REC_SYNC_H

#include <stdio.h>
#include <stdlib.h>
// ---------------------------- Functions Definitions ----------------------------
#define IS_LINE_BREAK(value) ( (value == '\r' || value == '\n')  ? 1 : 0)
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
struct Command * parserAOFCommand(size_t CHUNK_SIZE, char * BUFFER, size_t offset);
int read_positive_int(size_t CHUNK_SIZE, char * BUFFER, int * offset);
char * read_string(size_t CHUNK_SIZE, char * BUFFER, int * i, int size);
void print_command(struct Command * command);
void print_raw_chunk(size_t CHUNK_SIZE, char * BUFFER);
void print_parse_error(size_t CHUNK_SIZE, char * BUFFER, size_t offset);
size_t ASSERT_FILE(size_t value);
// -------------------------------------------------------------------------------

// ----------------------------- Structs Definitions -----------------------------
struct Command{
    size_t log_offset_start;
    size_t log_offset_end;
    int size;
    int number_of_tokens;
    char ** tokens;
};
// -------------------------------------------------------------------------------

// State Machine for analyzing log commands
#define STATE_ERROR  0 // START STATE
#define     STATE_1  1
#define     STATE_2  2
#define     STATE_3  3
#define     STATE_4  4
#define     STATE_5  5
#define     STATE_6  6 // FINAL STATE

struct Command * parserAOFCommand(size_t CHUNK_SIZE, char * BUFFER, size_t offset){
    int current_state = STATE_1;
    
    char ** tokens = NULL;

    int number_of_sentences = 0;
    int current_sentence_index = 0;
    int current_sentence_size = 0;
    char * current_sentence = NULL;

    for(size_t i = offset; i < CHUNK_SIZE; i++){
        if( IS_LINE_BREAK(BUFFER[i]) ){
            continue;
        }

        if(current_state == STATE_1){
            current_state = BUFFER[i] == '*' ? STATE_2 : STATE_ERROR;
            
           // printf("\nState 1: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_2){  
            number_of_sentences = read_positive_int(CHUNK_SIZE, BUFFER, &i);  
            if(number_of_sentences > 0){
                tokens = (char **) malloc( sizeof(char*) * number_of_sentences); 
                current_state = STATE_3;
            }else{
                current_state = STATE_ERROR;
            }

            //printf("\nState 2: BUFFER[%d]: %c -> number_of_sentences: %d ", i, BUFFER[i], number_of_sentences );
        }else

        if(current_state == STATE_3){
            current_state = BUFFER[i] == '$' ? STATE_4 : STATE_ERROR;
            
            //printf("\nState 3: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_4){
            current_sentence_size = read_positive_int(CHUNK_SIZE, BUFFER, &i);
            current_state = current_sentence_size > 0 ? STATE_5 : STATE_ERROR;
            
            //printf("\nState 4: BUFFER[%d]: %c -> current_sentence_size: %d ", i, BUFFER[i], current_sentence_size );
        }else

        if(current_state == STATE_5){
            current_sentence = read_string(CHUNK_SIZE, BUFFER, &i, current_sentence_size);
            if(current_sentence != NULL){
                tokens[current_sentence_index] = current_sentence;
                current_sentence_index++;
                current_state = STATE_6;
            }else{
                current_state = STATE_ERROR;
            }

            //printf("\nState 5: BUFFER[%d]: %c -> current_sentence: %s ", i, BUFFER[i], current_sentence );
        }else

        if(current_state == STATE_6){
            if(current_sentence_index == number_of_sentences){
                struct Command * command = (struct Command *) malloc(sizeof(struct Command));
                command->number_of_tokens = number_of_sentences;
                command->tokens = tokens;
                command->size = i - offset;
                command->log_offset_start = 0;
                command->log_offset_end = 0;
                return command; // FINAL STATE
            }
            current_state = BUFFER[i] == '$' ? STATE_4 : STATE_ERROR;

            //printf("\nState 6: BUFFER[%d]: %c -> %d sentences remain ", i, BUFFER[i], number_of_sentences - current_sentence_index );
        }else

        if(current_state == STATE_ERROR){
            //print_parse_error(CHUNK_SIZE, BUFFER, i);
            break;
        }

    }
    
    if(tokens != NULL){
        for(int index = 0; index < number_of_sentences; index++){
            if(tokens[index] != NULL){
                free(tokens[index]);
            }
            tokens[index] = NULL;
        }
        free(tokens);
    }

    return NULL;
}


char * read_string(size_t CHUNK_SIZE, char * BUFFER, int * i, int size){
    if(*i + size >= CHUNK_SIZE) return NULL;

    char * string = (char*) malloc(sizeof(size + 1));
    string[size] = '\0';
    
    int k = 0;
    while(k < size){
        string[k] = BUFFER[*i];
        *i = *i + 1;
        k = k + 1;
    }
    return string;
}

int read_positive_int(size_t CHUNK_SIZE, char * BUFFER, int * i){
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
    for(int i=0; i < CHUNK_SIZE;i++){
        printf("%c",BUFFER[i]);
    }
    printf("\n-------------------------------------------------------------\n");
}

void print_command(struct Command * command){
    printf("\n[ ");
    for(int i = 0; i < command->number_of_tokens; i++){
        printf("%s ",command->tokens[i]);
    }
    printf("](%d) size: %d, start: %ld, end: %ld", command->number_of_tokens, command->size, command->log_offset_start, command->log_offset_end);
    
}

size_t ASSERT_FILE(size_t value){
    if(value == (size_t)-1){
        perror("Redis AOF file is corrupted? ERROR");
    }
    return value;
}


#endif


