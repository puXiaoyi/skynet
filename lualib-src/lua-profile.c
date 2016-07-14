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

// ��ȡ��ǰ˫����ʱ������
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
// ���ܸ��ٿ�ʼ
static int
lstart(lua_State *L) {
	// Ĭ�ϵ�ǰ���̣߳�Ҳ���Դ���
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	// �ѵڶ�����ֵ����total timeֵѹջ
	lua_rawget(L, lua_upvalueindex(2));
	if (!lua_isnil(L, -1)) {
		return luaL_error(L, "Thread %p start profile more than once", lua_topointer(L, 1));
	}
	// ���õڶ�����ֵtotal timeΪ0
	lua_pushthread(L);
	lua_pushnumber(L, 0);
	lua_rawset(L, lua_upvalueindex(2));

	// ���õ�һ����ֵstart timeΪ��ǰʱ�� 
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
// ���ܸ��ٽ���
static int
lstop(lua_State *L) {
	// Ĭ�ϵ�ǰ���̣߳�Ҳ���Դ���
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	// �ѵ�һ����ֵ����start timeֵѹջ
	lua_rawget(L, lua_upvalueindex(1));
	if (lua_type(L, -1) != LUA_TNUMBER) {
		// ������stop֮ǰִ��start
		return luaL_error(L, "Call profile.start() before profile.stop()");
	} 
	// ����start time �뵱ǰʱ��Ĳ�ֵ
	double ti = diff_time(lua_tonumber(L, -1));
	// �ѵڶ�����ֵ����total timeֵѹջ
	lua_pushthread(L);
	lua_rawget(L, lua_upvalueindex(2));
	// ��ȡtotal time
	double total_time = lua_tonumber(L, -1);

	// ���õ�һ����ֵΪnil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(1));

	// ���õڶ�����ֵΪnil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(2));

	// ����totaltime��ѹ��ջ��Ϊlua����ֵ
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
	// �ѵڶ�����ֵ����total timeֵѹջ
	lua_rawget(L, lua_upvalueindex(2));
	if (lua_isnil(L, -1)) {		// check total time
		lua_pop(L,1);
	} else {
		// ֻ���total time�ǿգ���ȡֵ
		lua_pop(L,1);
		// �������߳�ѹջ
		lua_pushvalue(L,1);
		// ��ȡ��ǰʱ��
		double ti = get_time();
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] resume\n", from);
#endif
		// ���õ�һ����ֵΪ���µ�start time
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(1));	// set start time
	}

	// ��ȡ��������ֵ����coroutine.resume����
	lua_CFunction co_resume = lua_tocfunction(L, lua_upvalueindex(3));

	// ִ��coroutine.resume����
	return co_resume(L);
}

// profile.resume
// coroutine_resume skynet.lua
static int
lresume(lua_State *L) {
	// �ѵ�ǰ״̬�������߳�ѹջ
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
	// �ѵڶ�����ֵ����total timeֵѹջ
	lua_rawget(L, lua_upvalueindex(2));	// check total time
	if (lua_isnil(L, -1)) {
		lua_pop(L,1);
	} else {
		// ��ȡtotal time
		double ti = lua_tonumber(L, -1);
		lua_pop(L,1);

		// �ѵ�ǰ״̬�������߳�ѹջ
		lua_pushthread(L);
		// �ѵ�һ����ֵ��start timeֵѹջ
		lua_rawget(L, lua_upvalueindex(1));
		// ��ȡstart time
		double starttime = lua_tonumber(L, -1);
		lua_pop(L,1);

		// ����start time ��ֵ��������total time
		double diff = diff_time(starttime);
		ti += diff;
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] yield (%lf/%lf)\n", from, diff, ti);
#endif

		// �ѵ�ǰ״̬�������߳�ѹջ
		lua_pushthread(L);
		// ���õڶ�����ֵΪ���µ�total time
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(2));
	}

	// ��ȡ��������ֵ����coroutine.yield����
	lua_CFunction co_yield = lua_tocfunction(L, lua_upvalueindex(3));

	// ִ��coroutine.yield����
	return co_yield(L);
}

// profile.yield
// coroutine_yield skynet.lua
static int
lyield(lua_State *L) {
	// �ѵ�ǰ״̬�������߳�ѹջ
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
	// ����һ���µı���Ԥ�����㹻���������� l ���ݵĿռ䣨������䣩
	luaL_newlibtable(L,l);
	// ����һ���±���� thread->start time ��ֵ��
	lua_newtable(L);	// table thread->start time
	// ����һ���±���� thread->total time ��ֵ��
	lua_newtable(L);	// table thread->total time

	// ����һ������������һ�ݣ��ֱ�����Ϊ���������Ԫ��
	lua_newtable(L);	// weak table
	lua_pushliteral(L, "kv");
	lua_setfield(L, -2, "__mode");

	lua_pushvalue(L, -1);
	lua_setmetatable(L, -3); 
	lua_setmetatable(L, -3);

	// ��nilѹջռλ��Ϊ��������ֵԤ���ռ�
	lua_pushnil(L);	// cfunction (coroutine.resume or coroutine.yield)
	// ������ l �е����к���ע�ᵽջ���ı���
	// nup = 3 ������ֵ���ֱ�Ϊ starttime(1),totaltime(2),nil(3)
	luaL_setfuncs(L,l,3);

	// ��¼ע����ջ����
	int libtable = lua_gettop(L);

	// ��ȫ�ֱ���coroutineѹջ
	lua_getglobal(L, "coroutine");

	// �ѱ�׼����coroutine.resumeѹջ
	
	// ��coroutine[resume]ѹջ
	lua_getfield(L, -1, "resume");

	// ��ȡcoroutine.resume����
	lua_CFunction co_resume = lua_tocfunction(L, -1);
	if (co_resume == NULL)
		return luaL_error(L, "Can't get coroutine.resume");
	// ��coroutine[resume]����ջ
	lua_pop(L,1);

	//����ע����е�resume��resume_co�����ĵ�������ֵΪcoroutine.resume����

	lua_getfield(L, libtable, "resume");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "resume_co");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	// �ѱ�׼����coroutine.yieldѹջ

	// ��coroutine[yield]ѹջ
	lua_getfield(L, -1, "yield");

	// ��ȡcoroutine.yield����
	lua_CFunction co_yield = lua_tocfunction(L, -1);
	if (co_yield == NULL)
		return luaL_error(L, "Can't get coroutine.yield");
	// ��coroutine[yield]����ջ
	lua_pop(L,1);

	//����ע����е�yield��yield_co�����ĵ�������ֵΪcoroutine.yield����

	lua_getfield(L, libtable, "yield");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "yield_co");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	// ����ջ��Ϊע�����������������ջԪ��
	lua_settop(L, libtable);

	return 1;
}
