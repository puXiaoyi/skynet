#include <stdio.h>
#include <lua.h>
#include <lauxlib.h>

#include <time.h>

#if defined(__APPLE__)
#include <mach/task.h>
#include <mach/mach.h>
#endif

#define NANOSEC 1000000000
#define MICROSEC 1000000

// #define DEBUG_LOG

// 获取当前双精度时间秒数
static double
get_time() {
#if  !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);

	int sec = ti.tv_sec & 0xffff;
	int nsec = ti.tv_nsec;

	return (double)sec + (double)nsec / NANOSEC;	
#else
	struct task_thread_times_info aTaskInfo;
	mach_msg_type_number_t aTaskInfoCount = TASK_THREAD_TIMES_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t )&aTaskInfo, &aTaskInfoCount)) {
		return 0;
	}

	int sec = aTaskInfo.user_time.seconds & 0xffff;
	int msec = aTaskInfo.user_time.microseconds;

	return (double)sec + (double)msec / MICROSEC;
#endif
}

static inline double 
diff_time(double start) {
	double now = get_time();
	if (now < start) {
		return now + 0x10000 - start;
	} else {
		return now - start;
	}
}

// profile.start
// 性能跟踪开始
static int
lstart(lua_State *L) {
	// 默认当前主线程，也可以传参
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	// 把第二个上值表中total time值压栈
	lua_rawget(L, lua_upvalueindex(2));
	if (!lua_isnil(L, -1)) {
		return luaL_error(L, "Thread %p start profile more than once", lua_topointer(L, 1));
	}
	// 设置第二个上值total time为0
	lua_pushthread(L);
	lua_pushnumber(L, 0);
	lua_rawset(L, lua_upvalueindex(2));

	// 设置第一个上值start time为当前时间 
	lua_pushthread(L);
	double ti = get_time();
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] start\n", L);
#endif
	lua_pushnumber(L, ti);
	lua_rawset(L, lua_upvalueindex(1));

	return 0;
}

// profile.stop
// 性能跟踪结束
static int
lstop(lua_State *L) {
	// 默认当前主线程，也可以传参
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	// 把第一个上值表中start time值压栈
	lua_rawget(L, lua_upvalueindex(1));
	if (lua_type(L, -1) != LUA_TNUMBER) {
		// 必须在stop之前执行start
		return luaL_error(L, "Call profile.start() before profile.stop()");
	} 
	// 计算start time 与当前时间的差值
	double ti = diff_time(lua_tonumber(L, -1));
	// 把第二个上值表中total time值压栈
	lua_pushthread(L);
	lua_rawget(L, lua_upvalueindex(2));
	// 获取total time
	double total_time = lua_tonumber(L, -1);

	// 设置第一个上值为nil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(1));

	// 设置第二个上值为nil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(2));

	// 更新totaltime并压入栈作为lua返回值
	total_time += ti;
	lua_pushnumber(L, total_time);
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] stop (%lf / %lf)\n", L, ti, total_time);
#endif

	return 1;
}

static int
timing_resume(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	// 把第二个上值表中total time值压栈
	lua_rawget(L, lua_upvalueindex(2));
	if (lua_isnil(L, -1)) {		// check total time
		lua_pop(L,1);
	} else {
		// 只检查total time非空，不取值
		lua_pop(L,1);
		// 复制主线程压栈
		lua_pushvalue(L,1);
		// 获取当前时间
		double ti = get_time();
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] resume\n", from);
#endif
		// 设置第一个上值为最新的start time
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(1));	// set start time
	}

	// 获取第三个上值，即coroutine.resume方法
	lua_CFunction co_resume = lua_tocfunction(L, lua_upvalueindex(3));

	// 执行coroutine.resume方法
	return co_resume(L);
}

// profile.resume
// coroutine_resume skynet.lua
static int
lresume(lua_State *L) {
	// 把当前状态机的主线程压栈
	lua_pushvalue(L,1);
	
	return timing_resume(L);
}

static int
lresume_co(lua_State *L) {
	luaL_checktype(L, 2, LUA_TTHREAD);
	lua_rotate(L, 2, -1);

	return timing_resume(L);
}

static int
timing_yield(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	// 把第二个上值表中total time值压栈
	lua_rawget(L, lua_upvalueindex(2));	// check total time
	if (lua_isnil(L, -1)) {
		lua_pop(L,1);
	} else {
		// 获取total time
		double ti = lua_tonumber(L, -1);
		lua_pop(L,1);

		// 把当前状态机的主线程压栈
		lua_pushthread(L);
		// 把第一个上值中start time值压栈
		lua_rawget(L, lua_upvalueindex(1));
		// 获取start time
		double starttime = lua_tonumber(L, -1);
		lua_pop(L,1);

		// 计算start time 差值，并更新total time
		double diff = diff_time(starttime);
		ti += diff;
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] yield (%lf/%lf)\n", from, diff, ti);
#endif

		// 把当前状态机的主线程压栈
		lua_pushthread(L);
		// 设置第二个上值为最新的total time
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(2));
	}

	// 获取第三个上值，即coroutine.yield方法
	lua_CFunction co_yield = lua_tocfunction(L, lua_upvalueindex(3));

	// 执行coroutine.yield方法
	return co_yield(L);
}

// profile.yield
// coroutine_yield skynet.lua
static int
lyield(lua_State *L) {
	// 把当前状态机的主线程压栈
	lua_pushthread(L);

	return timing_yield(L);
}

// profile.yield_co
// skynet_yield coroutine.lua
static int
lyield_co(lua_State *L) {
	luaL_checktype(L, 1, LUA_TTHREAD);
	lua_rotate(L, 1, -1);
	
	return timing_yield(L);
}

// require "profile"
int
luaopen_profile(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "start", lstart },
		{ "stop", lstop },
		{ "resume", lresume },
		{ "yield", lyield },
		{ "resume_co", lresume_co },
		{ "yield_co", lyield_co },
		{ NULL, NULL },
	};
	// 创建一张新的表，并预分配足够保存下数组 l 内容的空间（但不填充）
	luaL_newlibtable(L,l);
	// 创建一张新表，存放 thread->start time 键值对
	lua_newtable(L);	// table thread->start time
	// 创建一张新表，存放 thread->total time 键值对
	lua_newtable(L);	// table thread->total time

	// 创建一张弱表，并复制一份，分别设置为以上两表的元表
	lua_newtable(L);	// weak table
	lua_pushliteral(L, "kv");
	lua_setfield(L, -2, "__mode");

	lua_pushvalue(L, -1);
	lua_setmetatable(L, -3); 
	lua_setmetatable(L, -3);

	// 把nil压栈占位，为第三个上值预留空间
	lua_pushnil(L);	// cfunction (coroutine.resume or coroutine.yield)
	// 把数组 l 中的所有函数注册到栈顶的表中
	// nup = 3 三个上值，分别为 starttime(1),totaltime(2),nil(3)
	luaL_setfuncs(L,l,3);

	// 记录注册表的栈索引
	int libtable = lua_gettop(L);

	// 把全局变量coroutine压栈
	lua_getglobal(L, "coroutine");

	// 把标准库里coroutine.resume压栈
	
	// 把coroutine[resume]压栈
	lua_getfield(L, -1, "resume");

	// 获取coroutine.resume方法
	lua_CFunction co_resume = lua_tocfunction(L, -1);
	if (co_resume == NULL)
		return luaL_error(L, "Can't get coroutine.resume");
	// 把coroutine[resume]弹出栈
	lua_pop(L,1);

	//设置注册表中的resume和resume_co方法的第三个上值为coroutine.resume方法

	lua_getfield(L, libtable, "resume");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "resume_co");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	// 把标准库里coroutine.yield压栈

	// 把coroutine[yield]压栈
	lua_getfield(L, -1, "yield");

	// 获取coroutine.yield方法
	lua_CFunction co_yield = lua_tocfunction(L, -1);
	if (co_yield == NULL)
		return luaL_error(L, "Can't get coroutine.yield");
	// 把coroutine[yield]弹出栈
	lua_pop(L,1);

	//设置注册表中的yield和yield_co方法的第三个上值为coroutine.yield方法

	lua_getfield(L, libtable, "yield");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "yield_co");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	// 设置栈顶为注册表索引，清理其他栈元素
	lua_settop(L, libtable);

	return 1;
}
