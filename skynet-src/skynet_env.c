#include "skynet.h"
#include "skynet_env.h"
#include "spinlock.h"

#include <lua.h>
#include <lauxlib.h>

#include <stdlib.h>
#include <assert.h>

struct skynet_env {
	struct spinlock lock;	// 回旋锁
	lua_State *L;		// lua状态机
};

static struct skynet_env *E = NULL;	//skynet环境实例

// 从lua状态机获取环境变量
const char * 
skynet_getenv(const char *key) {
	// 加锁
	SPIN_LOCK(E)

	lua_State *L = E->L;
	
	// 把key对应的全局变量压栈
	lua_getglobal(L, key);
	// 获取栈顶lua变量，转化为c字符串
	const char * result = lua_tostring(L, -1);
	// 使用完出栈
	lua_pop(L, 1);

	// 解锁
	SPIN_UNLOCK(E)

	return result;
}

// 设置环境变量
void 
skynet_setenv(const char *key, const char *value) {
	// 加锁
	SPIN_LOCK(E)
	
	lua_State *L = E->L;
	
	// 先判断key对应的全局变量是否为空
	lua_getglobal(L, key);
	assert(lua_isnil(L, -1));
	lua_pop(L,1);
	// 如果为空，把value值压栈
	lua_pushstring(L,value);
	// 弹出栈顶元素，并设置为key对应的全局变量值
	lua_setglobal(L,key);

	// 解锁
	SPIN_UNLOCK(E)
}

// skynet环境初始化
void
skynet_env_init() {
	// 分配内存空间
	E = skynet_malloc(sizeof(*E));
	// 初始化回旋锁
	SPIN_INIT(E)
	// 创建新的lua状态机
	E->L = luaL_newstate();
}
