#include "skynet.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// 内存报警阈值 32M
#define MEMORY_WARNING_REPORT (1024 * 1024 * 32)

// snlua结构
struct snlua {
	lua_State * L;					// lua虚拟机
	struct skynet_context * ctx;	// skynet服务
	size_t mem;						// 占用内存
	size_t mem_report;				// 内存报警阈值
	size_t mem_limit;				// 内存限制 
};

// LUA_CACHELIB may defined in patched lua for shared proto
#ifdef LUA_CACHELIB

#define codecache luaopen_cache

#else

static int
cleardummy(lua_State *L) {
  return 0;
}

static int 
codecache(lua_State *L) {
	// 注册方法集，必须以NULL,NULL结尾
	luaL_Reg l[] = {
		{ "clear", cleardummy },
		{ "mode", cleardummy },
		{ NULL, NULL },
	};
	// 创建一张新表，把方法集注册进去
	// 新表同时被压入栈中
	luaL_newlib(L,l);
	// 把全局变量loadfile压入栈中
	lua_getglobal(L, "loadfile");
	// 设置新表中索引loadfile的值为刚刚压入栈顶的全局变量loadfile的值
	lua_setfield(L, -2, "loadfile");
	return 1;
}

#endif

// 自定义错误处理函数
static int 
traceback (lua_State *L) {
	const char *msg = lua_tostring(L, 1);
	if (msg)
		luaL_traceback(L, L, msg, 1);
	else {
		lua_pushliteral(L, "(no error message)");
	}
	return 1;
}

static void
report_launcher_error(struct skynet_context *ctx) {
	// sizeof "ERROR" == 5
	skynet_sendname(ctx, 0, ".launcher", PTYPE_TEXT, 0, "ERROR", 5);
}

static const char *
optstring(struct skynet_context *ctx, const char *key, const char * str) {
	const char * ret = skynet_command(ctx, "GETENV", key);
	if (ret == NULL) {
		return str;
	}
	return ret;
}

// 回调函数逻辑
static int
init_cb(struct snlua *l, struct skynet_context *ctx, const char * args, size_t sz) {
	lua_State *L = l->L;
	l->ctx = ctx;
	// 停止GC
	lua_gc(L, LUA_GCSTOP, 0);
	// 设置 LUA_NOENV 为 true
	lua_pushboolean(L, 1);  /* signal for libraries to ignore env. vars. */
	lua_setfield(L, LUA_REGISTRYINDEX, "LUA_NOENV");
	// 打开状态机中的所有 Lua 标准库
	luaL_openlibs(L);
	// 把skynet服务压入栈，并设置为注册表变量skynet_context
	lua_pushlightuserdata(L, ctx);
	lua_setfield(L, LUA_REGISTRYINDEX, "skynet_context");
	// 如果 "skynet.codecache" 不在 package.loaded 中， 则调用函数 codecache ，并传入字符串 "skynet.codecache"。 将其返回值置入 package.loaded[skynet.codecache]
	luaL_requiref(L, "skynet.codecache", codecache , 0);
	// 清空栈
	lua_pop(L,1);
 
	// 设置 LUA_PATH、LUA_CPATH、LUA_SERVICE、LUA_PRELOAD 四个全局变量值
	const char *path = optstring(ctx, "lua_path","./lualib/?.lua;./lualib/?/init.lua");
	lua_pushstring(L, path);
	lua_setglobal(L, "LUA_PATH");
	const char *cpath = optstring(ctx, "lua_cpath","./luaclib/?.so");
	lua_pushstring(L, cpath);
	lua_setglobal(L, "LUA_CPATH");
	const char *service = optstring(ctx, "luaservice", "./service/?.lua");
	lua_pushstring(L, service);
	lua_setglobal(L, "LUA_SERVICE");
	const char *preload = skynet_command(ctx, "GETENV", "preload");
	lua_pushstring(L, preload);
	lua_setglobal(L, "LUA_PRELOAD");

	// 把c函数traceback压入栈
	lua_pushcfunction(L, traceback);
	// 保证c函数在栈顶
	assert(lua_gettop(L) == 1);

	const char * loader = optstring(ctx, "lualoader", "./lualib/loader.lua");

	// 加载loader文件
	// 从文件中加载lua代码并编译，编译成功后的程序块被压入栈中
	int r = luaL_loadfile(L,loader);
	if (r != LUA_OK) {
		skynet_error(ctx, "Can't load %s : %s", loader, lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}
	// 把参数字符串压入栈
	lua_pushlstring(L, args, sz);
	// 运行lua方法，这里为luaL_loadfile加载并编译成功的代码块
	// 参数为lualib中lua文件名，这里就是用自定义的loader代码加载特定的lualib文件
	// nargs = 1 一个参数
	// nresults = 0 没有返回值
	// msgh = 1 错误处理函数指向栈上索引为1的c函数traceback，如果为0则是默认的错误处理函数
	r = lua_pcall(L,1,0,1);
	if (r != LUA_OK) {
		skynet_error(ctx, "lua loader error : %s", lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}
	// 清空栈
	lua_settop(L,0);
	// 把注册表变量memlimit压入栈，判断类型是否为LUA_TNUMBER
	if (lua_getfield(L, LUA_REGISTRYINDEX, "memlimit") == LUA_TNUMBER) {
		// 获取memlimit值，判断内存报警
		size_t limit = lua_tointeger(L, -1);
		l->mem_limit = limit;
		skynet_error(ctx, "Set memory limit to %.2f M", (float)limit / (1024 * 1024));
		// 把nil压入栈，并设置为注册表变量memlimit
		lua_pushnil(L);
		lua_setfield(L, LUA_REGISTRYINDEX, "memlimit");
	}
	// 弹出注册表变量memlimit
	lua_pop(L, 1);

	// 重启GC
	lua_gc(L, LUA_GCRESTART, 0);

	return 0;
}

// 回调函数
static int
launch_cb(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz) {
	// 确保是第一条消息
	assert(type == 0 && session == 0);
	struct snlua *l = ud;
	// 清除回调函数
	skynet_callback(context, NULL, NULL);
	// 执行回调逻辑
	int err = init_cb(l, context, msg, sz);
	if (err) {
		// 如果出错，退出服务
		skynet_command(context, "EXIT", NULL);
	}

	return 0;
}

// 初始化snlua结构
int
snlua_init(struct snlua *l, struct skynet_context *ctx, const char * args) {
	int sz = strlen(args);
	char * tmp = skynet_malloc(sz);
	memcpy(tmp, args, sz);
	// 注册回调函数
	skynet_callback(ctx, l , launch_cb);
	// 传参为NULL，获取当前skynet服务":%x"格式的handle值
	const char * self = skynet_command(ctx, "REG", NULL);
	// 格式化为16进制整数
	uint32_t handle_id = strtoul(self+1, NULL, 16);
	// it must be first message
	// 给自己发第一条消息
	skynet_send(ctx, 0, handle_id, PTYPE_TAG_DONTCOPY,0, tmp, sz);
	return 0;
}

// 自定义内存分配器函数
// ud ，一个由 lua_newstate 传给它的指针
// ptr ，一个指向已分配出来/将被重新分配/要释放的内存块指针
// osize ，内存块原来的尺寸或是关于什么将被分配出来的代码
// nsize ，新内存块的尺寸
static void *
lalloc(void * ud, void *ptr, size_t osize, size_t nsize) {
	struct snlua *l = ud;
	size_t mem = l->mem;
	l->mem += nsize;
	if (ptr)
		l->mem -= osize;
	if (l->mem_limit != 0 && l->mem > l->mem_limit) {
		if (ptr == NULL || nsize > osize) {
			l->mem = mem;
			return NULL;
		}
	}
	if (l->mem > l->mem_report) {
		l->mem_report *= 2;
		skynet_error(l->ctx, "Memory warning %.2f M", (float)l->mem / (1024 * 1024));
	}
	return skynet_lalloc(ptr, osize, nsize);
}

// 创建snlua结构
struct snlua *
snlua_create(void) {
	struct snlua * l = skynet_malloc(sizeof(*l));
	memset(l,0,sizeof(*l));
	l->mem_report = MEMORY_WARNING_REPORT;
	l->mem_limit = 0;
	// 创建lua虚拟机
	l->L = lua_newstate(lalloc, l);
	return l;
}

// 关闭并释放lua状态机
void
snlua_release(struct snlua *l) {
	lua_close(l->L);
	skynet_free(l);
}

void
snlua_signal(struct snlua *l, int signal) {
	skynet_error(l->ctx, "recv a signal %d", signal);
	if (signal == 0) {
#ifdef lua_checksig
	// If our lua support signal (modified lua version by skynet), trigger it.
	skynet_sig_L = l->L;
#endif
	} else if (signal == 1) {
		skynet_error(l->ctx, "Current Memory %.3fK", (float)l->mem / 1024);
	}
}
