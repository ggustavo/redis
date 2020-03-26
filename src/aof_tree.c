
#include <stdlib.h>
#include <stdio.h>
#include <sys/file.h>  // To Open ?
#include <sys/types.h> // To Read and Write
#include <unistd.h>    // To Close
#include <fcntl.h>     // To Open ?
#include <unistd.h>    // To Sleep

#define AOF_TREE_FILE_NAME "aof.tree"
#define      READ_INTERVAL 10       // seconds
#define         CHUNK_SIZE 100     // bytes   32768


char * AOF_FILE_NAME;
long   LAST_END_FILE;
char * BUFFER;

// ---------------------------- Functions Definitions ----------------------------
#define ASSERT_FILE(value) (value == -1 ? ({perror("Redis AOF file is corrupted? ERROR"); exit(EXIT_FAILURE); } ) : (value))
#define IS_LINE_BREAK(value) ( (value == '\r' || value == '\n')  ? 1 : 0)
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
int run_cycle(int file, long file_size);
void print_raw_chunk();
struct Command * parserCommand(long offset);
void print_parse_error(int offset);
int read_positive_int(int * offset);
char * read_string(int * i, int size);
void print_command(struct Command * command);
// -------------------------------------------------------------------------------


int DEBUG_STATE_MACHINE = 0;
int DEBUG_CYCLE = 0;


// ----------------------------- Structs Definitions -----------------------------
struct Command{
    int log_offset_start;
    int log_offset_end;
    int size;
    int number_of_tokens;
    char ** tokens;
};
// -------------------------------------------------------------------------------


int main(int argc, char **argv){

    if(argc > 1){
        AOF_FILE_NAME = argv[1];
    }else{
        AOF_FILE_NAME = "appendonly.aof";
    }

    LAST_END_FILE = 0;
    BUFFER = (char*) malloc(CHUNK_SIZE);

    printf("\n------------------- Start Redis AOF TREE -------------------");
    printf("\n AOF_TREE_FILE_NAME:  %s",       AOF_TREE_FILE_NAME);
    printf("\n AOF_FILE_NAME:       %s",       AOF_FILE_NAME);
    printf("\n READ_INTERVAL:       %d MS",    READ_INTERVAL);
    printf("\n LAST_END_FILE:       %d bytes", LAST_END_FILE);
    printf("\n CHUNK_SIZE:          %d bytes", CHUNK_SIZE);
    printf("\n-------------------------------------------------------------");

 
    int print_wait = 1;
    int run = 0;

    int file = open(AOF_FILE_NAME,  O_RDWR, 0777);
	ASSERT_FILE(file);

    long file_size = lseek(file, 0, SEEK_END);
    ASSERT_FILE(file_size);

    while(run_cycle(file, file_size) != 0); // Read all log file    
    close(file);

    while(1){ // Read the log file at intervals
      
        
        if( run == 0 ) {
            if(print_wait == 1){
                printf("\n Waiting ...\n");
                fflush(stdout);
                print_wait = 0;
            }
        }else{
            print_wait = 1;
        }
        sleep(READ_INTERVAL);

        // ------------------------------------------ update file and file_size
        file = open(AOF_FILE_NAME,  O_RDWR, 0777); 
        file_size = lseek(file, 0, SEEK_END);
        // ------------------------------------------
        run = run_cycle(file, file_size);
        
        close(file);
    }

    return EXIT_SUCCESS;
}


int run_cycle(int file, long file_size){
    
     int interactions = (file_size - LAST_END_FILE) / CHUNK_SIZE;
    
    if(DEBUG_CYCLE) printf("\n\n----------------- Running Cycle (fd:%d) -------------------", file);
    if(DEBUG_CYCLE) printf("\n AOF_FILE_CURRENT_SIZE:  %d bytes", file_size);
    if(DEBUG_CYCLE) printf("\n LAST_END_FILE:          %d bytes", LAST_END_FILE);
    if(DEBUG_CYCLE) printf("\n NUMBER_OF_INTERACTIONS: %d",       interactions);
  
    long seek = 0;
    int bytes_read = 0;
    long current_offset = 0;

    for (int i = 0; i < interactions; i++) {    
        seek = lseek(file, LAST_END_FILE, SEEK_SET);
        ASSERT_FILE(seek);
        
        bytes_read = read(file, BUFFER, CHUNK_SIZE);
        ASSERT_FILE(bytes_read);
        if(bytes_read != CHUNK_SIZE){
            printf("\nERROR: failed to read a complete chuck %d/%d", bytes_read, CHUNK_SIZE);
            exit(EXIT_FAILURE);
        }


        //print_raw_chunk();
        current_offset = LAST_END_FILE;

        while(1){ // While we can parse commands

            struct Command * command = parserCommand(current_offset - LAST_END_FILE);
            if(command != NULL){
                command->log_offset_start = current_offset;
                command->log_offset_end = current_offset + command->size - 1; 
                current_offset  = current_offset + command->size; // Next OFFSET!
                print_command(command);
                printf("\n");
                fflush(stdout);
                
            }else{
                LAST_END_FILE = current_offset;
                if(DEBUG_CYCLE) printf("\nERROR: Failed to parse the last command. Opening a new chunk from the byte %d", LAST_END_FILE);
                break;
            }

        }
    }   
    return interactions;
}


// State Machine for analyzing log commands

#define STATE_ERROR  0 // START STATE
#define     STATE_1  1
#define     STATE_2  2
#define     STATE_3  3
#define     STATE_4  4
#define     STATE_5  5
#define     STATE_6  6 // FINAL STATE

struct Command * parserCommand(long offset){
    int current_state = STATE_1;
    
    char ** tokens = NULL;

    int number_of_sentences = 0;
    int current_sentence_index = 0;
    int current_sentence_size = 0;
    char * current_sentence = NULL;
    /*
    printf("\n");
    for(int i = offset; i < CHUNK_SIZE; i++){
        if( IS_LINE_BREAK(BUFFER[i]) ){
            BUFFER[i] = '^';
        }
        printf( "%c", BUFFER[i] );
    }
    printf("\n");
    */
    for(int i = offset; i < CHUNK_SIZE; i++){
        if( IS_LINE_BREAK(BUFFER[i]) ){
            continue;
        }

        if(current_state == STATE_1){
            current_state = BUFFER[i] == '*' ? STATE_2 : STATE_ERROR;
            
           if(DEBUG_STATE_MACHINE) printf("\nState 1: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_2){  
            number_of_sentences = read_positive_int(&i);  
            if(number_of_sentences > 0){
                tokens = (char **) malloc( sizeof(char*) * number_of_sentences); 
                current_state = STATE_3;
            }else{
                current_state = STATE_ERROR;
            }

            if(DEBUG_STATE_MACHINE) printf("\nState 2: BUFFER[%d]: %c -> number_of_sentences: %d ", i, BUFFER[i], number_of_sentences );
        }else

        if(current_state == STATE_3){
            current_state = BUFFER[i] == '$' ? STATE_4 : STATE_ERROR;
            
            if(DEBUG_STATE_MACHINE) printf("\nState 3: BUFFER[%d]: %c", i, BUFFER[i] );
        }else

        if(current_state == STATE_4){
            current_sentence_size = read_positive_int(&i);
            current_state = current_sentence_size > 0 ? STATE_5 : STATE_ERROR;
            
            if(DEBUG_STATE_MACHINE) printf("\nState 4: BUFFER[%d]: %c -> current_sentence_size: %d ", i, BUFFER[i], current_sentence_size );
        }else

        if(current_state == STATE_5){
            current_sentence = read_string(&i, current_sentence_size);
            if(current_sentence != NULL){
                tokens[current_sentence_index] = current_sentence;
                current_sentence_index++;
                current_state = STATE_6;
            }else{
                current_state = STATE_ERROR;
            }

            if(DEBUG_STATE_MACHINE) printf("\nState 5: BUFFER[%d]: %c -> current_sentence: %s ", i, BUFFER[i], current_sentence );
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

            if(DEBUG_STATE_MACHINE) printf("\nState 6: BUFFER[%d]: %c -> %d sentences remain ", i, BUFFER[i], number_of_sentences - current_sentence_index );
        }else

        if(current_state == STATE_ERROR){
            print_parse_error(i);
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


char * read_string(int * i, int size){
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

int read_positive_int(int * i){
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

void print_parse_error(int offset){
    if(!DEBUG_STATE_MACHINE) return;
    printf("\nParse ERROR at BUFFER[%d], start -> ", offset);
    for (int i = offset; i < MIN(offset + 20, CHUNK_SIZE); i++){
       printf("%c", BUFFER[i]);
    }
    printf("...\n");
    //exit(EXIT_FAILURE);
}

void print_raw_chunk(){

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
    printf("](%d) size: %d, start: %d, end: %d", command->number_of_tokens, command->size, command->log_offset_start, command->log_offset_end);
    
}