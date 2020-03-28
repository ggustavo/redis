#include "server.h"


#define INST_RECOVERY_STARTED 1
#define INST_RECOVERY_DONE    0

void instant_recovery_start(client *c){
    
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

