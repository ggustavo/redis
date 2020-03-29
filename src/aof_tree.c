#include "aof_inst_recovery_sync.h"

#include <sys/file.h>  // To Open ?
#include <sys/types.h> // To Read and Write
#include <unistd.h>    // To Close
#include <fcntl.h>     // To Open ?
#include <unistd.h>    // To Sleep

char * AOF_TREE_FILE_NAME = "aof.tree";
int READ_INTERVAL = 10;    // seconds
size_t CHUNK_SIZE = 100;     // bytes   32768
char * AOF_FILE_NAME;
size_t LAST_END_FILE;
char * BUFFER;

int DEBUG_CYCLE = 0;


int run_cycle(int file, size_t file_size){
    
     int interactions = (file_size - LAST_END_FILE) / CHUNK_SIZE;
    
    if(DEBUG_CYCLE) printf("\n\n----------------- Running Cycle (fd:%d) -------------------", file);
    if(DEBUG_CYCLE) printf("\n AOF_FILE_CURRENT_SIZE:  %ld bytes", file_size);
    if(DEBUG_CYCLE) printf("\n LAST_END_FILE:          %ld bytes", LAST_END_FILE);
    if(DEBUG_CYCLE) printf("\n NUMBER_OF_INTERACTIONS: %d",       interactions);
  
    size_t seek = 0;
    int bytes_read = 0;
    size_t current_offset = 0;

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

            struct Command * command = parserAOFCommand(CHUNK_SIZE, BUFFER, current_offset - LAST_END_FILE);
            if(command != NULL){
                command->log_offset_start = current_offset;
                command->log_offset_end = current_offset + command->size - 1; 
                current_offset  = current_offset + command->size; // Next OFFSET!
                print_command(command);
                printf("\n");
                fflush(stdout);
                
            }else{
                LAST_END_FILE = current_offset;
                if(DEBUG_CYCLE) printf("\nERROR: Failed to parse the last command. Opening a new chunk from the byte %ld", LAST_END_FILE);
                break;
            }

        }
    }   
    return interactions;
}


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
    printf("\n LAST_END_FILE:       %ld bytes", LAST_END_FILE);
    printf("\n CHUNK_SIZE:          %d bytes", CHUNK_SIZE);
    printf("\n-------------------------------------------------------------");

 
    int print_wait = 1;
    int run = 0;

    int file = open(AOF_FILE_NAME,  O_RDWR, 0777);
	ASSERT_FILE(file);

    size_t file_size = lseek(file, 0, SEEK_END);
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