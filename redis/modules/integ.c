#include "../redismodule.h"

int GetDb_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_ReplyWithLongLong(ctx, RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   if (RedisModule_Init(ctx, "integ", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
   }
   if (RedisModule_CreateCommand(ctx, "integ.getdb", GetDb_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
   }
   return REDISMODULE_OK;
}
