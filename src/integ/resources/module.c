#include "./redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

/* HELLO.SIMPLE is among the simplest commands you can implement.
 * It just returns the currently selected DB id, a functionality which is
 * missing in Redis. The command uses two important API calls: one to
 * fetch the currently selected DB, the other in order to send the client
 * an integer reply as response. */
int HelloSimple_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

   if (RedisModule_CreateCommand(ctx,"hello.simple", HelloSimple_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
   }

   return REDISMODULE_OK;
}
