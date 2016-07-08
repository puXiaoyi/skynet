#include "skynet.h"

#include "skynet_imp.h"
#include "skynet_env.h"
#include "skynet_server.h"
#include "luashrtbl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <signal.h>
#include <assert.h>

// 获取环境变量整数值
static int
optint(const char *key, int opt) {
	// 获取key对应的环境变量
	const char * str = skynet_getenv(key);
	if (str == NULL) {
		// 如果为空，设置该环境变量为默认值
		char tmp[20];
		sprintf(tmp,"%d",opt);
		skynet_setenv(key, tmp);
		return opt;
	}
	// 字符串转化为十进制整数
	return strtol(str, NULL, 10);
}

/*
static int
optboolean(const char *key, int opt) {
	const char * str = skynet_getenv(key);
	if (str == NULL) {
		skynet_setenv(key, opt ? "true" : "false");
		return opt;
	}
	return strcmp(str,"true")==0;
}
*/

// 获取环境变量字符串
static const char *
optstring(const char *key,const char * opt) {
	const char * str = skynet_getenv(key);
	if (str == NULL) {
		if (opt) {
			skynet_setenv(key, opt);
			opt = skynet_getenv(key);
		}
		return opt;
	}
	return str;
}

// 填充环境变量
static void
_init_env(lua_State *L) {
	// 把nil值压栈，为lua_next提供遍历条件
	lua_pushnil(L);  /* first key */
	// 弹出栈顶元素，把索引对应的表的键值对压栈（先键后值）
	while (lua_next(L, -2) != 0) {
		// 判断栈中key是否为字符串
		int keyt = lua_type(L, -2);
		if (keyt != LUA_TSTRING) {
			fprintf(stderr, "Invalid config table\n");
			exit(1);
		}
		// 取出栈中key的值
		const char * key = lua_tostring(L,-2);
		// 判断栈中value的值类型
		if (lua_type(L,-1) == LUA_TBOOLEAN) {
			int b = lua_toboolean(L,-1);
			skynet_setenv(key,b ? "true" : "false" );
		} else {
			const char * value = lua_tostring(L,-1);
			if (value == NULL) {
				fprintf(stderr, "Invalid config table key = %s\n", key);
				exit(1);
			}
			skynet_setenv(key,value);
		}
		// 弹出key，留下value，为lua_next提供遍历条件
		lua_pop(L,1);
	}
	// 遍历结束，弹出最后的value
	lua_pop(L,1);
}

// 信号处理
int sigign() {
	// 忽略管道中止信号
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sa, 0);
	return 0;
}

static const char * load_config = "\
	local config_name = ...\
	local f = assert(io.open(config_name))\
	local code = assert(f:read \'*a\')\
	local function getenv(name) return assert(os.getenv(name), \'os.getenv() failed: \' .. name) end\
	code = string.gsub(code, \'%$([%w_%d]+)\', getenv)\
	f:close()\
	local result = {}\
	assert(load(code,\'=(load)\',\'t\',result))()\
	return result\
";

// skynet程序入口函数
int
main(int argc, char *argv[]) {
	// 传入（第一个）参数：配置文件路径
	const char * config_file = NULL ;
	if (argc > 1) {
		config_file = argv[1];
	} else {
		fprintf(stderr, "Need a config file. Please read skynet wiki : https://github.com/cloudwu/skynet/wiki/Config\n"
			"usage: skynet configfilename\n");
		return 1;
	}

	luaS_initshr();
	// 初始化全局变量（主线程）
	skynet_globalinit();
	// 初始化环境变量
	skynet_env_init();
	// 信号处理
	sigign();

	// 装载配置信息
	struct skynet_config config;

	// 新建lua状态机
	struct lua_State *L = luaL_newstate();
	// 打开状态机里lua标准库
	luaL_openlibs(L);	// link lua lib

	// 加载lua代码块
	int err = luaL_loadstring(L, load_config);
	assert(err == LUA_OK);
	// 把配置参数压栈
	lua_pushstring(L, config_file);

	// 运行加载的lua代码，一个参数一个返回值
	err = lua_pcall(L, 1, 1, 0);
	if (err) {
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		lua_close(L);
		return 1;
	}
	// 填充skyent环境
	_init_env(L);

	config.thread =  optint("thread",8);
	config.module_path = optstring("cpath","./cservice/?.so");
	config.harbor = optint("harbor", 1);
	config.bootstrap = optstring("bootstrap","snlua bootstrap");
	config.daemon = optstring("daemon", NULL);
	config.logger = optstring("logger", NULL);
	config.logservice = optstring("logservice", "logger");

	lua_close(L);

	// 启动skynet
	skynet_start(&config);
	// 退出主线程
	skynet_globalexit();
	luaS_exitshr();

	return 0;
}
