#include "skynet.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// �ڴ汨����ֵ 32M
#define MEMORY_WARNING_REPORT (1024 * 1024 * 32)

// snlua�ṹ
struct snlua {
	lua_State * L;					// lua�����
	struct skynet_context * ctx;	// skynet����
	size_t mem;						// ռ���ڴ�
	size_t mem_report;				// �ڴ汨����ֵ
	size_t mem_limit;				// �ڴ����� 
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
	// ע�᷽������������NULL,NULL��β
	luaL_Reg l[] = {
		{ "clear", cleardummy },
		{ "mode", cleardummy },
		{ NULL, NULL },
	};
	// ����һ���±��ѷ�����ע���ȥ
	// �±�ͬʱ��ѹ��ջ��
	luaL_newlib(L,l);
	// ��ȫ�ֱ���loadfileѹ��ջ��
	lua_getglobal(L, "loadfile");
	// �����±�������loadfile��ֵΪ�ո�ѹ��ջ����ȫ�ֱ���loadfile��ֵ
	lua_setfield(L, -2, "loadfile");
	return 1;
}

#endif

// �Զ����������
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

// �ص������߼�
static int
init_cb(struct snlua *l, struct skynet_context *ctx, const char * args, size_t sz) {
	lua_State *L = l->L;
	l->ctx = ctx;
	// ֹͣGC
	lua_gc(L, LUA_GCSTOP, 0);
	// ���� LUA_NOENV Ϊ true
	lua_pushboolean(L, 1);  /* signal for libraries to ignore env. vars. */
	lua_setfield(L, LUA_REGISTRYINDEX, "LUA_NOENV");
	// ��״̬���е����� Lua ��׼��
	luaL_openlibs(L);
	// ��skynet����ѹ��ջ��������Ϊע������skynet_context
	lua_pushlightuserdata(L, ctx);
	lua_setfield(L, LUA_REGISTRYINDEX, "skynet_context");
	// ��� "skynet.codecache" ���� package.loaded �У� ����ú��� codecache ���������ַ��� "skynet.codecache"�� ���䷵��ֵ���� package.loaded[skynet.codecache]
	luaL_requiref(L, "skynet.codecache", codecache , 0);
	// ���ջ
	lua_pop(L,1);
 
	// ���� LUA_PATH��LUA_CPATH��LUA_SERVICE��LUA_PRELOAD �ĸ�ȫ�ֱ���ֵ
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

	// ��c����tracebackѹ��ջ
	lua_pushcfunction(L, traceback);
	// ��֤c������ջ��
	assert(lua_gettop(L) == 1);

	const char * loader = optstring(ctx, "lualoader", "./lualib/loader.lua");

	// ����loader�ļ�
	// ���ļ��м���lua���벢���룬����ɹ���ĳ���鱻ѹ��ջ��
	int r = luaL_loadfile(L,loader);
	if (r != LUA_OK) {
		skynet_error(ctx, "Can't load %s : %s", loader, lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}
	// �Ѳ����ַ���ѹ��ջ
	lua_pushlstring(L, args, sz);
	// ����lua����������ΪluaL_loadfile���ز�����ɹ��Ĵ����
	// ����Ϊlualib��lua�ļ���������������Զ����loader��������ض���lualib�ļ�
	// nargs = 1 һ������
	// nresults = 0 û�з���ֵ
	// msgh = 1 ��������ָ��ջ������Ϊ1��c����traceback�����Ϊ0����Ĭ�ϵĴ�������
	r = lua_pcall(L,1,0,1);
	if (r != LUA_OK) {
		skynet_error(ctx, "lua loader error : %s", lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}
	// ���ջ
	lua_settop(L,0);
	// ��ע������memlimitѹ��ջ���ж������Ƿ�ΪLUA_TNUMBER
	if (lua_getfield(L, LUA_REGISTRYINDEX, "memlimit") == LUA_TNUMBER) {
		// ��ȡmemlimitֵ���ж��ڴ汨��
		size_t limit = lua_tointeger(L, -1);
		l->mem_limit = limit;
		skynet_error(ctx, "Set memory limit to %.2f M", (float)limit / (1024 * 1024));
		// ��nilѹ��ջ��������Ϊע������memlimit
		lua_pushnil(L);
		lua_setfield(L, LUA_REGISTRYINDEX, "memlimit");
	}
	// ����ע������memlimit
	lua_pop(L, 1);

	// ����GC
	lua_gc(L, LUA_GCRESTART, 0);

	return 0;
}

// �ص�����
static int
launch_cb(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz) {
	// ȷ���ǵ�һ����Ϣ
	assert(type == 0 && session == 0);
	struct snlua *l = ud;
	// ����ص�����
	skynet_callback(context, NULL, NULL);
	// ִ�лص��߼�
	int err = init_cb(l, context, msg, sz);
	if (err) {
		// ��������˳�����
		skynet_command(context, "EXIT", NULL);
	}

	return 0;
}

// ��ʼ��snlua�ṹ
int
snlua_init(struct snlua *l, struct skynet_context *ctx, const char * args) {
	int sz = strlen(args);
	char * tmp = skynet_malloc(sz);
	memcpy(tmp, args, sz);
	// ע��ص�����
	skynet_callback(ctx, l , launch_cb);
	// ����ΪNULL����ȡ��ǰskynet����":%x"��ʽ��handleֵ
	const char * self = skynet_command(ctx, "REG", NULL);
	// ��ʽ��Ϊ16��������
	uint32_t handle_id = strtoul(self+1, NULL, 16);
	// it must be first message
	// ���Լ�����һ����Ϣ
	skynet_send(ctx, 0, handle_id, PTYPE_TAG_DONTCOPY,0, tmp, sz);
	return 0;
}

// �Զ����ڴ����������
// ud ��һ���� lua_newstate ��������ָ��
// ptr ��һ��ָ���ѷ������/�������·���/Ҫ�ͷŵ��ڴ��ָ��
// osize ���ڴ��ԭ���ĳߴ���ǹ���ʲô������������Ĵ���
// nsize �����ڴ��ĳߴ�
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

// ����snlua�ṹ
struct snlua *
snlua_create(void) {
	struct snlua * l = skynet_malloc(sizeof(*l));
	memset(l,0,sizeof(*l));
	l->mem_report = MEMORY_WARNING_REPORT;
	l->mem_limit = 0;
	// ����lua�����
	l->L = lua_newstate(lalloc, l);
	return l;
}

// �رղ��ͷ�lua״̬��
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
