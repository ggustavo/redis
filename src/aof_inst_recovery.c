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
// -----------------------------------------------------------------
MDB_env *env = NULL;
MDB_dbi dbi;
MDB_txn * sync_transaction;
pthread_t ptid; 
long sync_commands = 0;

void instant_recovery_receiver_command(robj **argv, int argc){
    if( argc == 3){
        sync_commands++;
       // printf("\nC(%ld):%s K(%ld): %s V(%ld): %s " , sdslen(argv[0]->ptr), argv[0]->ptr
       //                                             , sdslen(argv[1]->ptr), argv[1]->ptr
       //                                             , sdslen(argv[2]->ptr), argv[2]->ptr
       //                                           ); 
        MDB_val key;
        key.mv_data = argv[1]->ptr;
        key.mv_size = sdslen(argv[1]->ptr);

        MDB_val val;
        val.mv_data = argv[2]->ptr;
        val.mv_size = sdslen(argv[2]->ptr);

        E(mdb_put(sync_transaction, dbi, &key, &val, 0));
    }
 
}

robj* instant_recovery_restore_record_by_key(redisDb *db, robj *key){
    if(server.aof_in_instant_recovery_process != C_OK){
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

        serverLog(LL_NOTICE, "Instant Recovery Key find");
        robj *val = createStringObject(mb_val.mv_data, mb_val.mv_size);
        dbAdd(db, key, val);
        return val;

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
   
    server.aof_in_instant_recovery_process =  C_OK;
    serverLog(LL_NOTICE, "Instant Recovery Start NOW !!");
    
    long long start = ustime();
    
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

    int find_keys = 0;


    while ( mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) { //restore
        robj *key_r = createStringObject(key.mv_data, key.mv_size);
        robj *val_r = createStringObject(val.mv_data, val.mv_size);
        
        find_keys++;
        
        if(!dictFind(c->db->dict,key_r->ptr)){
            dbAdd(c->db, key_r, val_r);
        }
	}

	mdb_cursor_close(cursor);
	mdb_txn_abort(txn); 

    server.aof_in_instant_recovery_process = C_ERR;
    serverLog(LL_NOTICE,"Instant Recovery Finish %.3f seconds (read %d keys)",(float)(ustime()-start)/1000000, find_keys);

    addReply(c, shared.ok);
   // pthread_create(&ptid, NULL, &instant_recovery_start_cycle_thread, NULL);
}

void instant_recovery_start_cycle_thread(void* arg){
    /*
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
    */
}

MDB_env* instant_recovery_create_env(){
	MDB_env *env;
	E(mdb_env_create(&env));
    E(system("mkdir -p data"));
    E(mdb_env_open(env, "data", 0, 0664));
    mdb_env_set_mapsize(env, 10000000 * 100);
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

void instant_recovery_sync_index(){

    env = instant_recovery_create_env();
    dbi = instant_recovery_create_dbi(env);

    serverLog(LL_NOTICE,"aof_current_size: %ld", server.aof_current_size);
    serverLog(LL_NOTICE,"aof_fsync_offset: %ld", server.aof_fsync_offset);
    serverLog(LL_NOTICE,"aof_rewrite_base_size: %ld", server.aof_rewrite_base_size);
    
    E(mdb_txn_begin(env, NULL, 0, &sync_transaction));   
    if (loadAppendOnlyFile(server.aof_filename) == C_ERR){
        serverLog(LL_NOTICE,"DB loaded from append only file error");
    }
    E(mdb_txn_commit(sync_transaction));
    
    serverLog(LL_NOTICE,"instant recovery sync %d commands", sync_commands);
    printf("\n");   

}

int E(int rc){
    if(rc != 0){
        fprintf(stderr, "\n mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
        exit(EXIT_FAILURE);
    } 
    return rc;
}

int isValidCommand(char * token, int size){
    UNUSED(token);
    if( size <= 4) return 1;
    return 0;
}