#include "skynet.h"
#include "lua-seri.h"

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"

#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct snlua {
	lua_State * L;
	struct skynet_context * ctx;
	const char * preload;
};

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

// �ص�����
// typedef int (*skynet_cb)(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz);
// 
static int
_cb(struct skynet_context * context, void * ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	lua_State *L = ud;
	int trace = 1;
	int r;
	int top = lua_gettop(L);
	if (top == 0) {
		// ���luaջΪ�գ���һ������
		// ��ѹ��c����traceback
		lua_pushcfunction(L, traceback);
		// �ٰ�ע������_cbѹ��ջ��
		lua_rawgetp(L, LUA_REGISTRYINDEX, _cb);
	} else {
		// ���ջ��Ϊ�գ��������2��Ԫ�أ���һ������ѹ��ջ�е�traceback��_cb
		assert(top == 2);
	}
	// ���ƻص�������ѹ��ջ��
	lua_pushvalue(L,2);

	// ����ѹ�����ֵ
	lua_pushinteger(L, type);
	lua_pushlightuserdata(L, (void *)msg);
	lua_pushinteger(L,sz);
	lua_pushinteger(L, session);
	lua_pushinteger(L, source);

	// ִ��lua�ص�����
	// skynet.lua �е� raw_dispatch_message(prototype, msg, sz, session, source)
	r = lua_pcall(L, 5, 0 , trace);

	if (r == LUA_OK) {
		return 0;
	}
	const char * self = skynet_command(context, "REG", NULL);
	switch (r) {
	case LUA_ERRRUN:
		skynet_error(context, "lua call [%x to %s : %d msgsz = %d] error : " KRED "%s" KNRM, source , self, session, sz, lua_tostring(L,-1));
		break;
	case LUA_ERRMEM:
		skynet_error(context, "lua memory error : [%x to %s : %d]", source , self, session);
		break;
	case LUA_ERRERR:
		skynet_error(context, "lua error in error : [%x to %s : %d]", source , self, session);
		break;
	case LUA_ERRGCMM:
		skynet_error(context, "lua gc error : [%x to %s : %d]", source , self, session);
		break;
	};

	// �����ص���������������traceback��_cb
	lua_pop(L,1);

	return 0;
}

static int
forward_cb(struct skynet_context * context, void * ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	_cb(context, ud, type, session, source, msg, sz);
	// don't delete msg in forward mode.
	return 1;
}

// c.callback(cb, forward)
static int
lcallback(lua_State *L) {
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ�ڶ�������ֵ
	int forward = lua_toboolean(L, 2);
	// ����һ�������Ƿ�Ϊ��������
	luaL_checktype(L,1,LUA_TFUNCTION);
	// ����ջ��Ϊ1
	lua_settop(L,1);
	// ��ջ������Ԫ������Ϊע������_cb
	lua_rawsetp(L, LUA_REGISTRYINDEX, _cb);

	// ��ע������״̬�������߳�ѹ��ջ��
	lua_rawgeti(L, LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
	// ��ջ�л�ȡ״̬��
	lua_State *gL = lua_tothread(L,-1);

	// ע��ص�����
	if (forward) {
		skynet_callback(context, gL, forward_cb);
	} else {
		skynet_callback(context, gL, _cb);
	}

	return 0;
}

// c.command(cmd, parm)
// ���� local addr = c.command("QUERY", name)
// name Ϊ string��addr Ϊ string
static int
lcommand(lua_State *L) {
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ��һ������ֵ
	const char * cmd = luaL_checkstring(L,1);
	const char * result;
	const char * parm = NULL;
	if (lua_gettop(L) == 2) {
		// ���ջ��Ϊ2����ȡ�ڶ�������
		// ���ջԪ�������Ƿ�Ϊstring������������ַ���
		parm = luaL_checkstring(L,2);
	}

	// ������Ӧ��ָ���
	result = skynet_command(context, cmd, parm);
	if (result) {
		// ����з���ֵ��ѹ��ֵ��Ϊlua����ֵ
		lua_pushstring(L, result);
		return 1;
	}
	return 0;
}

// c.intcommand(cmd, parm)
// ���� local session = c.intcommand("TIMEOUT", ti)
// parm Ϊ integer��session ҲΪ integer
static int
lintcommand(lua_State *L) {
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ��һ������ֵ
	const char * cmd = luaL_checkstring(L,1);
	const char * result;
	const char * parm = NULL;
	char tmp[64];	// for integer parm
	if (lua_gettop(L) == 2) {
		// ���ջ��Ϊ2����ȡ�ڶ�������
		int32_t n = (int32_t)luaL_checkinteger(L,2);
		sprintf(tmp, "%d", n);
		parm = tmp;
	}

	result = skynet_command(context, cmd, parm);
	if (result) {
		// ����з���ֵ��ѹ��ջ��Ϊlua����ֵ
		lua_Integer r = strtoll(result, NULL, 0);
		lua_pushinteger(L, r);
		return 1;
	}
	return 0;
}

// c.genid
static int
lgenid(lua_State *L) { 
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ������µ�sessionֵ����ѹ��ջ��Ϊlua����ֵ
	int session = skynet_send(context, 0, 0, PTYPE_TAG_ALLOCSESSION , 0 , NULL, 0);
	lua_pushinteger(L, session);
	return 1;
}

// ��ȡ��ַ���ַ���
static const char *
get_dest_string(lua_State *L, int index) {
	const char * dest_string = lua_tostring(L, index);
	if (dest_string == NULL) {
		luaL_error(L, "dest address type (%s) must be a string or number.", lua_typename(L, lua_type(L,index)));
	}
	return dest_string;
}

/*
	uint32 address
	 string address
	integer type
	integer session
	string message
	 lightuserdata message_ptr
	 integer len
 */
// local session = c.send(addr, p.id , nil , p.pack(...))
static int
lsend(lua_State *L) {
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ��һ������ֵ��Ŀ�ĵص�ַ
	// �ȳ��Ի�ȡ�����͵�ַ
	uint32_t dest = (uint32_t)lua_tointeger(L, 1);
	const char * dest_string = NULL;
	if (dest == 0) {
		// ����������͵�ַ����ȡ��ַ���ַ���
		if (lua_type(L,1) == LUA_TNUMBER) {
			return luaL_error(L, "Invalid service address 0");
		}
		dest_string = get_dest_string(L, 1);
	}

	// ��ȡ�ڶ�������ֵ����Ϣ����
	int type = luaL_checkinteger(L, 2);
	int session = 0;
	if (lua_isnil(L,3)) {
		// �������������Ϊnil�����Զ������µ�session
		type |= PTYPE_TAG_ALLOCSESSION;
	} else {
		// �����ȡsessionֵ
		session = luaL_checkinteger(L,3);
	}

	// ��ȡ���ĸ�����ֵ
	int mtype = lua_type(L,4);
	switch (mtype) {
	case LUA_TSTRING: {
		// ������ַ���
		size_t len = 0;
		void * msg = (void *)lua_tolstring(L,4,&len);
		if (len == 0) {
			msg = NULL;
		}
		if (dest_string) {
			session = skynet_sendname(context, 0, dest_string, type, session , msg, len);
		} else {
			session = skynet_send(context, 0, dest, type, session , msg, len);
		}
		break;
	}
	case LUA_TLIGHTUSERDATA: {
		// ������������û����� msg, sz
		void * msg = lua_touserdata(L,4);
		int size = luaL_checkinteger(L,5);
		if (dest_string) {
			session = skynet_sendname(context, 0, dest_string, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		} else {
			session = skynet_send(context, 0, dest, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		}
		break;
	}
	default:
		luaL_error(L, "skynet.send invalid param %s", lua_typename(L, lua_type(L,4)));
	}
	if (session < 0) {
		// send to invalid address
		// todo: maybe throw an error would be better
		return 0;
	}
	// sessionֵѹջ��Ϊlua����ֵ
	lua_pushinteger(L,session);
	return 1;
}

// c.redirect(address, 0, skynet.PTYPE_ERROR, session, "")
static int
lredirect(lua_State *L) {
	// ��ȡ��һ����ֵ��skynet_context
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	// ��ȡ��һ������ֵ��Ŀ�ĵص�ַ
	// �ȳ��Ի�ȡ�����͵�ַ
	uint32_t dest = (uint32_t)lua_tointeger(L,1);
	const char * dest_string = NULL;
	if (dest == 0) {
		// ����������͵�ַ����ȡ��ַ���ַ���
		dest_string = get_dest_string(L, 1);
	}
	// ��ȡ�ڶ�������ֵ��Դ��ַ
	uint32_t source = (uint32_t)luaL_checkinteger(L,2);
	// ��ȡ�ڶ�������ֵ������
	int type = luaL_checkinteger(L,3);
	// ��ȡ�ڶ�������ֵ��session
	int session = luaL_checkinteger(L,4);

	// ͬlsend����
	int mtype = lua_type(L,5);
	switch (mtype) {
	case LUA_TSTRING: {
		size_t len = 0;
		void * msg = (void *)lua_tolstring(L,5,&len);
		if (len == 0) {
			msg = NULL;
		}
		if (dest_string) {
			session = skynet_sendname(context, source, dest_string, type, session , msg, len);
		} else {
			session = skynet_send(context, source, dest, type, session , msg, len);
		}
		break;
	}
	case LUA_TLIGHTUSERDATA: {
		void * msg = lua_touserdata(L,5);
		int size = luaL_checkinteger(L,6);
		if (dest_string) {
			session = skynet_sendname(context, source, dest_string, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		} else {
			session = skynet_send(context, source, dest, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		}
		break;
	}
	default:
		luaL_error(L, "skynet.redirect invalid param %s", lua_typename(L,mtype));
	}
	// û��lua����ֵ
	return 0;
}

static int
lerror(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	int n = lua_gettop(L);
	if (n <= 1) {
		lua_settop(L, 1);
		const char * s = luaL_tolstring(L, 1, NULL);
		skynet_error(context, "%s", s);
		return 0;
	}
	luaL_Buffer b;
	luaL_buffinit(L, &b);
	int i;
	for (i=1; i<=n; i++) {
		luaL_tolstring(L, i, NULL);
		luaL_addvalue(&b);
		if (i<n) {
			luaL_addchar(&b, ' ');
		}
	}
	luaL_pushresult(&b);
	skynet_error(context, "%s", lua_tostring(L, -1));
	return 0;
}

// c.tostring
// skynet.tostring
// userdata(msg) + integer(sz) -> string(str)
static int
ltostring(lua_State *L) {
	if (lua_isnoneornil(L,1)) {
		return 0;
	}
	char * msg = lua_touserdata(L,1);
	int sz = luaL_checkinteger(L,2);
	lua_pushlstring(L,msg,sz);
	return 1;
}

static int
lharbor(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	uint32_t handle = (uint32_t)luaL_checkinteger(L,1);
	int harbor = 0;
	int remote = skynet_isremote(context, handle, &harbor);
	lua_pushinteger(L,harbor);
	lua_pushboolean(L, remote);

	return 2;
}

// c.packstring
// skynet.packstring
// userdata(str) + integer(sz) -> string(str)
static int
lpackstring(lua_State *L) {
	luaseri_pack(L);
	char * str = (char *)lua_touserdata(L, -2);
	int sz = lua_tointeger(L, -1);
	lua_pushlstring(L, str, sz);
	skynet_free(str);
	return 1;
}

// c.trash
// skynet.trash
static int
ltrash(lua_State *L) {
	int t = lua_type(L,1);
	switch (t) {
	case LUA_TSTRING: {
		break;
	}
	case LUA_TLIGHTUSERDATA: {
		void * msg = lua_touserdata(L,1);
		luaL_checkinteger(L,2);
		skynet_free(msg);
		break;
	}
	default:
		luaL_error(L, "skynet.trash invalid param %s", lua_typename(L,t));
	}

	return 0;
}

// c.now
// skynet.now
static int
lnow(lua_State *L) {
	uint64_t ti = skynet_now();
	lua_pushinteger(L, ti);
	return 1;
}

// require "skynet.core"
int
luaopen_skynet_core(lua_State *L) {
	// �����������ں��Ƿ��Ǵ������ Lua ״̬�����ںˣ��Լ��������Ĵ����Ƿ�ʹ������ͬ�� Lua �汾��ͬʱҲ�����������ں��봴���� Lua ״̬�����ں� �Ƿ�ʹ����ͬһƬ��ַ�ռ䡣
	luaL_checkversion(L);

	luaL_Reg l[] = {
		{ "send" , lsend },
		{ "genid", lgenid },
		{ "redirect", lredirect },
		{ "command" , lcommand },
		{ "intcommand", lintcommand },
		{ "error", lerror },
		{ "tostring", ltostring },
		{ "harbor", lharbor },
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "packstring", lpackstring },
		{ "trash" , ltrash },
		{ "callback", lcallback },
		{ "now", lnow },
		{ NULL, NULL },
	};

	// ����һ���µı���Ԥ�����㹻���������� l ���ݵĿռ䣨������䣩
	luaL_newlibtable(L, l);

	// ��ע������skynet_contextѹ��ջ�У���Ϊ����ע�ắ������ֵ
	lua_getfield(L, LUA_REGISTRYINDEX, "skynet_context");
	// ���snlua�����init_cb����������ִ�У���ȡ����ctxӦ�ò�Ϊ��
	struct skynet_context *ctx = lua_touserdata(L,-1);
	if (ctx == NULL) {
		return luaL_error(L, "Init skynet context first");
	}

	// ������ l �е����к���ע�ᵽջ���ı���
	// nup = 1 һ����ֵ��skynet_context
	// ��ѹ����ѹ��ֵ��ע����ϣ���ֵ��ջ�е���
	luaL_setfuncs(L,l,1);

	return 1;
}
