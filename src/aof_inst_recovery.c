#include "server.h"
#include "lmdb.h"


#define INST_RECOVERY_STARTED 1
#define INST_RECOVERY_DONE    0

int E(int rc){
    if(rc != 0) 
    fprintf(stderr, "\n mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
    return rc;
}

MDB_env* mdb_create_env(){
	MDB_env *env;
	E(mdb_env_create(&env));
    E(mdb_env_open(env, "data", 0, 0664));
    return env;
}

MDB_dbi mdb_create_dbi(MDB_env *env){
    MDB_dbi dbi;
    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));
    E(mdb_txn_commit(txn));
    return dbi;
}


void mdb_exec(MDB_env *env, MDB_dbi dbi, void * key_val,  void * data_val, int key_size, int data_size){
    MDB_val key;
	key.mv_size = key_size;
	key.mv_data = key_val;
	
    MDB_val data;
    data.mv_size = data_size;
	data.mv_data = data_val;
    
    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_put(txn, dbi, &key, &data, 0));
    E(mdb_txn_commit(txn));
} 

char * int_to_char(int number) {
    char * data = malloc(4);
	data[3] = (number >> 24) & 0xFF;
	data[2] = (number >> 16) & 0xFF;
	data[1] = (number >> 8)  & 0xFF;
	data[0] = (number)       & 0xFF;
    return data;
}
int char_to_int(char * data) {
	char buffer[4];
	buffer[3] = data[3];
	buffer[2] = data[2];
	buffer[1] = data[1];
	buffer[0] = data[0];
	return *(int*) buffer;
}


void mdb_all(MDB_env *env, MDB_dbi dbi){
    MDB_cursor *cursor;
	MDB_val key, data;
    MDB_txn *txn;
    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    E(mdb_cursor_open(txn, dbi, &cursor));
	
    while ( mdb_cursor_get(cursor, &key, &data, MDB_NEXT) == 0) {
        printf("\nKey: %d Value: %d", char_to_int(key.mv_data), char_to_int(data.mv_data));
	}
	mdb_cursor_close(cursor);
	//mdb_txn_abort(txn); //??
    E(mdb_txn_commit(txn));
}

void instant_recovery_start(client *c){

    MDB_env *env = mdb_create_env();
    MDB_dbi dbi  = mdb_create_dbi(env);
    
    char * key = NULL;
    for(int i = 0; i < 10; i++){
        key = int_to_char(i);
        mdb_exec(env, dbi, key, int_to_char(i*i), 4, 4);
        free(key);
    }
    mdb_all(env, dbi);

    server.aof_in_instant_recovery_process = INST_RECOVERY_STARTED;

    if(c->id == CLIENT_ID_AOF){
        serverLog(LL_NOTICE, "[ERROR] Instant Recovery called from AOF Load");
        addReply(c, shared.ok);
        return;
    }
    serverLog(LL_NOTICE, "Start Instant Recovery NOW !!");
    
    struct redisCommand * instant_cmd = c->cmd;
    robj **               instant_argv = c->argv;
    int                   instant_argc = c->argc;


    FILE *fp = fopen(server.aof_filename,"r");
    long offset = 23;

    // START RECOVERY COMMANDS HERE ------------------------------------
    if(instant_recovery_read_command(c,fp,offset) == C_OK){
        c->cmd->proc(c); //EXEC COMMAND!!
        instant_recovery_free_command(c);
    }else{
        instant_recovery_free_command(c);
    }
    fclose(fp);
    // FINISH RECOVERY

    
    c->argc = instant_argc;
    c->argv = instant_argv;
    c->cmd  = instant_cmd;

    server.aof_in_instant_recovery_process = INST_RECOVERY_DONE;
    addReply(c, shared.ok);
}


int instant_recovery_read_command(client *c, FILE * fp, long offset) {
    
    int argc, j;
    unsigned long len;
    robj **argv;
    char buf[128];
    sds argsds;
    struct redisCommand *cmd;

    fseek(fp, offset, 0);
    
    if (fgets(buf,sizeof(buf),fp) == NULL) {
        if (feof(fp)){
            serverLog(LL_WARNING, "[WARN] End of file");
            return C_ERR;
        }else{
            serverLog(LL_WARNING, "[ERRO] Read AOF File ERROR");
            return C_ERR;
        }
    }

    if (buf[0] != '*' || buf[1] == '\0'){
        serverLog(LL_WARNING, "[ERRO] Parse Command miss '*' or invalid char");
        return C_ERR;
    } 
    argc = atoi(buf+1);
    if (argc < 1){
        serverLog(LL_WARNING, "[ERRO] Parse Command argc <= 0");
        return C_ERR;
    }

    argv = zmalloc(sizeof(robj*)*argc);
    c->argc = argc; 
    c->argv = argv; // Commands (robj)

    for (j = 0; j < argc; j++) {
        /* Parse the argument len. */
        char *readres = fgets(buf,sizeof(buf),fp);

        if (readres == NULL || buf[0] != '$') {
            c->argc = j; /* Free up to j-1. */

            if (readres == NULL){
                serverLog(LL_WARNING, "[ERRO] Read AOF File ERROR readres == NULL");
                return C_ERR;
            }else{
                serverLog(LL_WARNING, "[ERRO] Parse Command: ---> %c <---", buf[0]);
                return C_ERR;
            }
        }
        
        len = strtol(buf+1,NULL,10);

        /* Read it into a string object. */
        argsds = sdsnewlen(SDS_NOINIT,len);
        
        if (len && fread(argsds,len,1,fp) == 0) {
            serverLog(LL_WARNING, "[ERRO] Parse Command: Read len");
            return C_ERR;
        }
      
        argv[j] = createObject(OBJ_STRING,argsds);

        /* Discard CRLF. */
        if (fread(buf,2,1,fp) == 0) {
            return C_ERR;
        }
    }

    /* Command lookup */
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
        serverLog(LL_WARNING,"Unknown command '%s' reading the append only file",(char*)argv[0]->ptr);
        exit(1);
    }
    c->cmd = cmd;
    return C_OK;
}

void instant_recovery_free_command(struct client *c) {
    if(c->argv == NULL)return;
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
    c->cmd = NULL;
}

