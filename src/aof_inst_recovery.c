#include "server.h"
#include "lmdb.h"
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>

// -----------------------------------------------------------------
int E(int rc);
MDB_env* instant_recovery_create_env();
MDB_dbi instant_recovery_create_dbi(MDB_env *env);
int isValidCommand(char * token, int size){
    UNUSED(token);
    if( size <= 4) return 1;
    return 0;
}
//char * int_to_char(int number);
//int char_to_int(char * data);
// -----------------------------------------------------------------
MDB_env *env = NULL;
MDB_dbi dbi;
MDB_txn * sync_transaction;
size_t CHUNK_SIZE = 15100;
pthread_t ptid; 
long restored_commadns = 0;

void receiver_command(struct Command * command){
    
    if(isValidCommand(command->tokens[0], command->tokens_size[0])){
       // print_aov_command(command);
        MDB_val key;
        key.mv_data = command->tokens[1];
        key.mv_size = command->tokens_size[1];
        
        int page_size = sizeof(int) + (sizeof(int) * command->number_of_tokens) + command->tokens_total_size;
        sds page = sdsnewlen(SDS_NOINIT, page_size);
        memcpy(page, &command->number_of_tokens, sizeof(int));
        int offset = sizeof(int) + ( sizeof(int) * command->number_of_tokens );
        for(int i = 1; i <= command->number_of_tokens; i++){
            memcpy(page + (i * sizeof(int)), 
            &command->tokens_size[i-1], sizeof(int));
            memcpy(page + offset, command->tokens[i-1], command->tokens_size[i-1]);
            offset = offset + command->tokens_size[i-1];
        }
        
        MDB_val val;
        val.mv_data = page;
        val.mv_size = page_size;
        E(mdb_put(sync_transaction, dbi, &key, &val, 0));
        restored_commadns ++;
        sdsfree(page);
    }
    free_tokens(command->tokens, command->tokens_size, command->number_of_tokens);
    zfree(command);

}


void instant_recovery_sync_index(){

    FILE * fp = instant_recovery_special_start_AOF();
    
    env = instant_recovery_create_env();
    dbi = instant_recovery_create_dbi(env);

    int file = fileno(fp); 
	ASSERT_FILE(file);   
    
    size_t file_size = lseek(file, 0, SEEK_END);
    ASSERT_FILE(file_size);

    serverLog(LL_NOTICE,"aof_current_size: %ld", server.aof_current_size);
    serverLog(LL_NOTICE,"aof_fsync_offset: %ld", server.aof_fsync_offset);
    serverLog(LL_NOTICE,"aof_rewrite_base_size: %ld", server.aof_rewrite_base_size);

    if(file_size > 0){
        size_t LAST_END_FILE = server.aof_current_size;
        char * BUFFER = (char*) zmalloc(CHUNK_SIZE);


        E(mdb_txn_begin(env, NULL, 0, &sync_transaction));
        int t = 0;
        do{
            LAST_END_FILE = run_cycle(
                            file, 
                            file_size, 
                            BUFFER, 
                            CHUNK_SIZE, 
                            LAST_END_FILE, 
                            receiver_command); // Read log file  
                            //t++; if(t==2)
                            //break;
        } while(LAST_END_FILE < file_size-1);
        
        E(mdb_txn_commit(sync_transaction));
        zfree(BUFFER);
        serverLog(LL_NOTICE,"instant recovery restore %d commands", restored_commadns);
        //close(file);
    }
   
    printf("\n");   
    instant_recovery_special_stop_AOF(fp);
}


robj* instant_recovery_get_record(redisDb *db, robj *key){
    if(server.aof_in_instant_recovery_process != INST_RECOVERY_STARTED){
        return NULL;
    }
    
    MDB_val mb_key;
	mb_key.mv_data = key->ptr;
	mb_key.mv_size = sdslen((sds)key->ptr);
    MDB_val mb_val;

    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    int rs = mdb_get(txn, dbi, &mb_key, &mb_val);
    mdb_txn_abort(txn);
    
    if(rs == 0){ // KEY FIND!!! let's recovery!!!
        int argc = 0;
        memcpy(&argc, mb_val.mv_data, sizeof(int));
        serverLog(LL_NOTICE, "Instant Recovery Key find %d", argc);
        if(argc == 3){ //SET COMMAND
            int val_size = 0;
            memcpy(&val_size, mb_val.mv_data + sizeof(int)*3, sizeof(int));
            int offset = mb_val.mv_size - val_size;
            robj *val = createStringObject(mb_val.mv_data + offset, val_size);
            dbAdd(db, key, val);
           // dictEntry *inst_de = dictFind(db->dict,key->ptr);
            //if(!inst_de){
           //     serverLog(LL_NOTICE, "ERRRORR!! %d");
           //    return NULL;
            //}
            return val;

        }
        return NULL;
        
    }else if(rs == MDB_NOTFOUND){ // It's ok! this is a new key
        return NULL;
    }
    serverLog(LL_NOTICE, "[ERROR] Instant Recovery MDB get record");
    return NULL;
}

void instant_recovery_start(client *c){

    if(c->id == CLIENT_ID_AOF){
        serverLog(LL_NOTICE, "[ERROR] Instant Recovery called from AOF Load?");
        addReply(c, shared.ok);
        return;
    }
   
    server.aof_in_instant_recovery_process =  INST_RECOVERY_STARTED;
 
    serverLog(LL_NOTICE, "Start Instant Recovery NOW !!");
    
    struct redisCommand * instant_cmd = c->cmd;
    robj ** instant_argv = c->argv;
    int     instant_argc = c->argc;
    int     replstate = c->replstate;

    c->argc = -1;
    c->argv = NULL;
    c->cmd  = NULL;
    //c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;

    if(env == NULL){
        serverLog(LL_NOTICE, "[ERROR] Instant Recovery evn = NULL trying open again");
        env = instant_recovery_create_env();
        dbi = instant_recovery_create_dbi(env);
    }

    MDB_cursor *cursor;
	MDB_val key, val;
    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    E(mdb_cursor_open(txn, dbi, &cursor));

	

    // while ( mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) { //restore
    //     //printf("\nKey: %s val_size %d", key.mv_data, val.mv_size);
    //     instant_recovery_set_redis_command(val.mv_data, c);
    //     c->cmd->proc(c);
    //     instant_recovery_free_command(c);
	// }

     while ( mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) { //restore
        //printf("\nKey: %s val_size %d", key.mv_data, val.mv_size);
        int val_inter_size = 0;
        memcpy(&val_inter_size, val.mv_data + sizeof(int)*3, sizeof(int));
        int offset = val.mv_size - val_inter_size;
        robj *key_r = createStringObject(key.mv_data, key.mv_size);
        robj *val_r = createStringObject(val.mv_data + offset, val_inter_size);
        dbAdd(c->db, key_r, val_r);
	}

	mdb_cursor_close(cursor);
	mdb_txn_abort(txn); 

    c->argc = instant_argc;
    c->argv = instant_argv;
    c->cmd  = instant_cmd;
    c->replstate = replstate;
    server.aof_in_instant_recovery_process = INST_RECOVERY_DONE;
    addReply(c, shared.ok);
    pthread_create(&ptid, NULL, &instant_recovery_cycle_thread, NULL);
}

void instant_recovery_cycle_thread(void* arg){
    UNUSED(arg);
    pthread_detach(pthread_self()); 

    serverLog(LL_NOTICE,"instant recovery cycle_thread last Size: %ld (started)", server.aof_current_size);
  
    char * BUFFER = (char*) zmalloc(CHUNK_SIZE);
    int file;
    size_t file_size;
    size_t LAST_END_FILE = server.aof_current_size;
    while(1){
        file = open(server.aof_filename, 02, 0777); 
        ASSERT_FILE(file);   
        
        file_size = lseek(file, 0, SEEK_END);
        ASSERT_FILE(file_size);

        if(file_size > 0 && LAST_END_FILE != file_size){
           
            E(mdb_txn_begin(env, NULL, 0, &sync_transaction));   
            
            do{
                LAST_END_FILE = run_cycle(
                                file, 
                                file_size, 
                                BUFFER, 
                                CHUNK_SIZE, 
                                LAST_END_FILE, 
                                receiver_command); // Read log file    
            } while(LAST_END_FILE < file_size-1);
                
            
            E(mdb_txn_commit(sync_transaction));
        }
        close(file);
        sleep(10);
        serverLog(LL_NOTICE,"instant recovery cycle_thread last size: %ld", server.aof_current_size);
    }
    zfree(BUFFER);
    pthread_exit(NULL); 
}

void instant_recovery_set_redis_command(sds page, client *c){
  
    struct redisCommand *cmd;
    int argc = 0;
    memcpy(&argc, page, sizeof(int));
    robj ** argv = zmalloc(sizeof(robj*)*argc);
    c->argc = argc; 
    c->argv = argv;

    printf("\n[");
    int offset = sizeof(int) + (sizeof(int) * argc);
    int current_token_size = 0;
    sds argsds = NULL;

    for(int i = 1; i <= argc; i++){
        
        memcpy(&current_token_size, page + (i * sizeof(int)), sizeof(int));
        argsds = sdsnewlen(SDS_NOINIT, current_token_size);
        memcpy(argsds, page + offset, current_token_size);
        offset = offset + current_token_size;
        argv[i-1] = createObject(OBJ_STRING,argsds);

        printf(" %s (%d)", argsds, current_token_size);

    }

    cmd = lookupCommand(argv[0]->ptr);
    if(!cmd) {
        serverLog(LL_WARNING, "Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
        exit(1);
    }
    printf(" name: %s | ", cmd->name);
  
    c->cmd = cmd;
}


void instant_recovery_free_command(struct client *c) {
    if(c->argv == NULL)return;
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
    c->cmd = NULL;
}

int E(int rc){
    if(rc != 0) 
    fprintf(stderr, "\n mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
    return rc;
}

MDB_env* instant_recovery_create_env(){
	MDB_env *env;
	E(mdb_env_create(&env));
    E(system("mkdir -p data"));
    E(mdb_env_open(env, "data", 0, 0664));
    return env;
}

MDB_dbi instant_recovery_create_dbi(MDB_env *env){
    MDB_dbi dbi;
    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));
    E(mdb_txn_commit(txn));
    return dbi;
}

int old_aof_state = 0;
 FILE * instant_recovery_special_start_AOF(){

    FILE *fp = fopen(server.aof_filename,"r");
    struct redis_stat sb;
    old_aof_state = server.aof_state;

    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        server.aof_fsync_offset = server.aof_current_size;
        return fp;
    }
    
    startLoadingFile(fp, server.aof_filename, RDBFLAGS_AOF_PREAMBLE);

    /* Check if this AOF file has an RDB preamble. In that case we need to
     * load the RDB file and later continue loading the AOF tail. */
    char sig[5]; /* "REDIS" */
    if (fread(sig,1,5,fp) != 5 || memcmp(sig,"REDIS",5) != 0) {
        /* No RDB preamble, seek back at 0 offset. */
        if (fseek(fp,0,SEEK_SET) == -1){
            fclose(fp);
            serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
            exit(1);
        }
    } else {
        /* RDB preamble. Pass loading the RDB functions. */
        rio rdb;

        serverLog(LL_NOTICE,"Reading RDB preamble from AOF file...");
        if (fseek(fp,0,SEEK_SET) == -1){
            fclose(fp);
            serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
            exit(1);
        }
        rioInitWithFile(&rdb,fp);
        if (rdbLoadRio(&rdb,RDBFLAGS_AOF_PREAMBLE,NULL) != C_OK) {
            serverLog(LL_WARNING,"Error reading the RDB preamble of the AOF file, AOF loading aborted");
            fclose(fp);
            serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
            exit(1);
        } else {
            serverLog(LL_NOTICE,"Reading the remaining AOF tail...");
        }
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    server.aof_state = AOF_OFF;

    return fp;
}


void instant_recovery_special_stop_AOF(FILE * fp){
    fclose(fp);
    server.aof_state = old_aof_state;
    stopLoading(1);
    //aofUpdateCurrentSize(){
    
    struct redis_stat sb;
    mstime_t latency;

    latencyStartMonitor(latency);
    if (redis_fstat(server.aof_fd,&sb) == -1) {
        serverLog(LL_WARNING,"Unable to obtain the AOF file length. stat: %s",
            strerror(errno));
    } else {
        server.aof_current_size = sb.st_size;
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat",latency);
    server.aof_rewrite_base_size = server.aof_current_size;
    server.aof_fsync_offset = server.aof_current_size;
}