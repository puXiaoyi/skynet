#include "skynet.h"

#include "skynet_server.h"
#include "skynet_module.h"
#include "skynet_handle.h"
#include "skynet_mq.h"
#include "skynet_timer.h"
#include "skynet_harbor.h"
#include "skynet_env.h"
#include "skynet_monitor.h"
#include "skynet_imp.h"
#include "skynet_log.h"
#include "spinlock.h"
#include "atomic.h"

#include <pthread.h>

#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>

#ifdef CALLING_CHECK

#define CHECKCALLING_BEGIN(ctx) if (!(spinlock_trylock(&ctx->calling))) { assert(0); }
#define CHECKCALLING_END(ctx) spinlock_unlock(&ctx->calling);
#define CHECKCALLING_INIT(ctx) spinlock_init(&ctx->calling);
#define CHECKCALLING_DESTROY(ctx) spinlock_destroy(&ctx->calling);
#define CHECKCALLING_DECL struct spinlock calling;

#else

#define CHECKCALLING_BEGIN(ctx)
#define CHECKCALLING_END(ctx)
#define CHECKCALLING_INIT(ctx)
#define CHECKCALLING_DESTROY(ctx)
#define CHECKCALLING_DECL

#endif

// skynet上下文的基本结构
struct skynet_context {
	void * instance;		//skynet_module实例，通过skynet_module_instance_create创建
	struct skynet_module * mod;	//skynet_module实体，封装了create init release signal四个基本方法
	void * cb_ud;			//回调的上下文，比如 gate logger snlua harbor lua_State NULL 等
	skynet_cb cb;			//回调函数，typedef int (*skynet_cb)(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz);
	struct message_queue *queue;	//消息队列
	FILE * logfile;			//日志指针
	char result[32];		//skynet_command 指令操作结果缓冲区
	uint32_t handle;		//唯一句柄，通过skynet_handle_register分配
	int session_id;			//会话id
	int ref;			//引用计数，初始为2
	bool init;			//初始化成功标识，初始为false，skynet_module_instance_init返回0时赋值为true
	bool endless;			//无限循环标识，monitor检测到版本长期未变化时赋值为true

	CHECKCALLING_DECL
};

struct skynet_node {
	int total;			//成功创建的skynet_context总数
	int init;			//是否初始化设置了线程私有变量handle值
	uint32_t monitor_exit;		//退出时收到消息的监控上下文handle，具体参见handle_exit
	pthread_key_t handle_key; 	//通过pthread_getpecific和pthread_setspecific实现同一个线程中不同函数间共享数据
};

// skynet_node实例，通过skynet_globalinit初始化
static struct skynet_node G_NODE;

// 创建的skynet_context总数
int 
skynet_context_total() {
	return G_NODE.total;
}

// 创建的skynet_context总数原子递增
static void
context_inc() {
	ATOM_INC(&G_NODE.total);
}

// 创建的skynet_context总数原子递减
static void
context_dec() {
	ATOM_DEC(&G_NODE.total);
}

// 通过获取线程私有变量得到当前线程中skynet_context的handle值
uint32_t 
skynet_current_handle(void) {
	if (G_NODE.init) {
		void * handle = pthread_getspecific(G_NODE.handle_key);
		return (uint32_t)(uintptr_t)handle;
	} else {
		// ?
		uint32_t v = (uint32_t)(-THREAD_MAIN);
		return v;
	}
}

// 格式化十进制数字为十六进制字符串
static void
id_to_hex(char * str, uint32_t id) {
	int i;
	static char hex[16] = { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F' };
	str[0] = ':';
	for (i=0;i<8;i++) {
		str[i+1] = hex[(id >> ((7-i) * 4))&0xf];
	}
	str[9] = '\0';
}

struct drop_t {
	uint32_t handle;
};

static void
drop_message(struct skynet_message *msg, void *ud) {
	struct drop_t *d = ud;
	skynet_free(msg->data);
	uint32_t source = d->handle;
	assert(source);
	// report error to the message source
	skynet_send(NULL, source, msg->source, PTYPE_ERROR, 0, NULL, 0);
}

// 创建一个skynet_context
struct skynet_context * 
skynet_context_new(const char * name, const char *param) {
	// 查询name对应的skynet_module实体
	struct skynet_module * mod = skynet_module_query(name);

	if (mod == NULL)
		return NULL;

	// 创建一个skynet_module实例
	void *inst = skynet_module_instance_create(mod);
	if (inst == NULL)
		return NULL;
	// 分配内存空间，创建新的skynet_context
	struct skynet_context * ctx = skynet_malloc(sizeof(*ctx));
	// 初始化回旋锁
	CHECKCALLING_INIT(ctx)

	// 初始化变量
	ctx->mod = mod;
	ctx->instance = inst;
	ctx->ref = 2;
	ctx->cb = NULL;
	ctx->cb_ud = NULL;
	ctx->session_id = 0;
	ctx->logfile = NULL;

	ctx->init = false;
	ctx->endless = false;
	// Should set to 0 first to avoid skynet_handle_retireall get an uninitialized handle
	ctx->handle = 0;
	// 分配一个新的唯一的句柄	
	ctx->handle = skynet_handle_register(ctx);
	// 创建一个新的消息队列
	struct message_queue * queue = ctx->queue = skynet_mq_create(ctx->handle);
	// init function maybe use ctx->handle, so it must init at last
	// 创建的skynet_context总数原子递增
	context_inc();

	// 加锁
	CHECKCALLING_BEGIN(ctx)
	// 初始化skynet_module实例
	int r = skynet_module_instance_init(mod, inst, ctx, param);
	// 解锁
	CHECKCALLING_END(ctx)
	if (r == 0) {
		// 初始化skynet_module成功，释放一个引用并修改初始化标识
		struct skynet_context * ret = skynet_context_release(ctx);
		if (ret) {
			ctx->init = true;
		}
		// 把消息队列加入全局消息队列
		skynet_globalmq_push(queue);
		if (ret) {
			skynet_error(ret, "LAUNCH %s %s", name, param ? param : "");
		}
		return ret;
	} else {
		skynet_error(ctx, "FAILED launch %s", name);
		uint32_t handle = ctx->handle;
		skynet_context_release(ctx);
		skynet_handle_retire(handle);
		struct drop_t d = { handle };
		skynet_mq_release(queue, drop_message, &d);
		return NULL;
	}
}

// 返回递增的session值
int
skynet_context_newsession(struct skynet_context *ctx) {
	// session always be a positive number
	int session = ++ctx->session_id;
	if (session <= 0) {
		ctx->session_id = 1;
		return 1;
	}
	return session;
}

// 每获取一次skynet_context，引用计数递增一次
void 
skynet_context_grab(struct skynet_context *ctx) {
	ATOM_INC(&ctx->ref);
}

void
skynet_context_reserve(struct skynet_context *ctx) {
	skynet_context_grab(ctx);
	// don't count the context reserved, because skynet abort (the worker threads terminate) only when the total context is 0 .
	// the reserved context will be release at last.
	context_dec();
}

// 销毁skynet_context
static void 
delete_context(struct skynet_context *ctx) {
	// 关闭文件，释放内存，销毁回旋锁
	if (ctx->logfile) {
		fclose(ctx->logfile);
	}
	skynet_module_instance_release(ctx->mod, ctx->instance);
	skynet_mq_mark_release(ctx->queue);
	CHECKCALLING_DESTROY(ctx)
	skynet_free(ctx);
	context_dec();
}

// 释放一个引用，当引用数为零时销毁skynet_context
struct skynet_context * 
skynet_context_release(struct skynet_context *ctx) {
	if (ATOM_DEC(&ctx->ref) == 0) {
		delete_context(ctx);
		return NULL;
	}
	return ctx;
}

// 往skynet_context压入一条skynet_message
int
skynet_context_push(uint32_t handle, struct skynet_message *message) {
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		return -1;
	}
	skynet_mq_push(ctx->queue, message);
	skynet_context_release(ctx);

	return 0;
}

// 设置skynet_context的endless标识为true
void 
skynet_context_endless(uint32_t handle) {
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		return;
	}
	ctx->endless = true;
	skynet_context_release(ctx);
}

int 
skynet_isremote(struct skynet_context * ctx, uint32_t handle, int * harbor) {
	int ret = skynet_harbor_message_isremote(handle);
	if (harbor) {
		*harbor = (int)(handle >> HANDLE_REMOTE_SHIFT);
	}
	return ret;
}

// 消息分发
static void
dispatch_message(struct skynet_context *ctx, struct skynet_message *msg) {
	// skynet_context必须已成功初始化
	assert(ctx->init);
	// 加锁
	CHECKCALLING_BEGIN(ctx)
	// 通过获取线程私有变量得到skynet_context的handle值
	pthread_setspecific(G_NODE.handle_key, (void *)(uintptr_t)(ctx->handle));
	// msg.sz 分离出type和sz，高8位为type
	int type = msg->sz >> MESSAGE_TYPE_SHIFT;
	size_t sz = msg->sz & MESSAGE_TYPE_MASK;
	// 日志输出
	if (ctx->logfile) {
		skynet_log_output(ctx->logfile, msg->source, type, msg->session, msg->data, sz);
	}
	// 执行回调函数
	if (!ctx->cb(ctx, ctx->cb_ud, type, msg->session, msg->source, msg->data, sz)) {
		// 返回0执行成功，由接收方释放消息数据内存空间
		skynet_free(msg->data);
	} 
	// 解锁
	CHECKCALLING_END(ctx)
}

// bootstrap 中调用，bootstrap启动失败的时候分发logger消息
void 
skynet_context_dispatchall(struct skynet_context * ctx) {
	// for skynet_error
	struct skynet_message msg;
	struct message_queue *q = ctx->queue;
	while (!skynet_mq_pop(q,&msg)) {
		dispatch_message(ctx, &msg);
	}
}

// thread_worker 中调用，工作线程循环执行进行消息分发
struct message_queue * 
skynet_context_message_dispatch(struct skynet_monitor *sm, struct message_queue *q, int weight) {
	if (q == NULL) {
		// 如果队列为空，从全局队列中弹出一个
		q = skynet_globalmq_pop();
		if (q==NULL)
			return NULL;
	}

	// 通过message_queue找到关联的handle
	uint32_t handle = skynet_mq_handle(q);

	// 通过handle找到所属的skynet_context
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		struct drop_t d = { handle };
		skynet_mq_release(q, drop_message, &d);
		return skynet_globalmq_pop();
	}

	int i,n=1;
	struct skynet_message msg;

	for (i=0;i<n;i++) {
		// 从消息队列q中弹出一个消息，如果消息队列为空返回1，否则正常返回0
		if (skynet_mq_pop(q,&msg)) {
			// 如果当前消息队列为空，弹出下一个上下文消息队列并返回
			skynet_context_release(ctx);
			return skynet_globalmq_pop();
		} else if (i==0 && weight >= 0) {
			// weight -1 0 1 2 3
			n = skynet_mq_length(q);
			n >>= weight;
		}
		// 如果过载阈值发生扩容，输出报警日志
		int overload = skynet_mq_overload(q);
		if (overload) {
			skynet_error(ctx, "May overload, message queue length = %d", overload);
		}

		// 更新监视器记录版本数及其他变量
		skynet_monitor_trigger(sm, msg.source , handle);

		if (ctx->cb == NULL) {
			// 如果回调函数为空，释放消息内存空间
			skynet_free(msg.data);
		} else {
			// 如果回调函数不为空，调用消息分发函数
			dispatch_message(ctx, &msg);
		}

		skynet_monitor_trigger(sm, 0,0);
	}

	assert(q == ctx->queue);
	// 如果全局队列不为空，则弹出下一个消息队列进行消息分发
	// 如果全局队列为空，则继续对当前消息队列进行消息分发
	struct message_queue *nq = skynet_globalmq_pop();
	if (nq) {
		// If global mq is not empty , push q back, and return next queue (nq)
		// Else (global mq is empty or block, don't push q back, and return q again (for next dispatch)
		// 把当前消息队列再压入全局队列 
		skynet_globalmq_push(q);
		// 返回下一个消息队列进行消息分发
		q = nq;
	} 
	skynet_context_release(ctx);

	return q;
}

// 复制地址名
static void
copy_name(char name[GLOBALNAME_LENGTH], const char * addr) {
	int i;
	for (i=0;i<GLOBALNAME_LENGTH && addr[i];i++) {
		name[i] = addr[i];
	}
	for (;i<GLOBALNAME_LENGTH;i++) {
		name[i] = '\0';
	}
}

// 返回名字对应的上下文服务
uint32_t 
skynet_queryname(struct skynet_context * context, const char * name) {
	switch(name[0]) {
	case ':':
		// 16进制数字地址
		return strtoul(name+1,NULL,16);
	case '.':
		// handle_storage中查找
		return skynet_handle_findname(name + 1);
	}
	skynet_error(context, "Don't support query global name %s",name);
	return 0;
}

// 退出handle对应上下文服务
static void
handle_exit(struct skynet_context * context, uint32_t handle) {
	// 输出退出信息
	if (handle == 0) {
		handle = context->handle;
		skynet_error(context, "KILL self");
	} else {
		skynet_error(context, "KILL :%0x", handle);
	}
	if (G_NODE.monitor_exit) {
		skynet_send(context,  handle, G_NODE.monitor_exit, PTYPE_CLIENT, 0, NULL, 0);
	}
	// 回收handle
	skynet_handle_retire(handle);
}

// skynet command

// 指令方法结构
struct command_func {
	const char *name;
	const char * (*func)(struct skynet_context * context, const char * param);
};

// 定时指令
static const char *
cmd_timeout(struct skynet_context * context, const char * param) {
	char * session_ptr = NULL;
	// 10进制的定时时间
	int ti = strtol(param, &session_ptr, 10); 
	int session = skynet_context_newsession(context);
	// 注册一个定期器事件
	skynet_timeout(context->handle, ti, session);
	sprintf(context->result, "%d", session);
	return context->result;
}

// 注册指令
static const char *
cmd_reg(struct skynet_context * context, const char * param) {
	if (param == NULL || param[0] == '\0') {
		// 不传参，直接返回上下文的handle值
		sprintf(context->result, ":%x", context->handle);
		return context->result;
	} else if (param[0] == '.') {
		// 传名字参数，为上下文设置name
		return skynet_handle_namehandle(context->handle, param + 1);
	} else {
		// 无效参数
		skynet_error(context, "Can't register global name %s in C", param);
		return NULL;
	}
}

// 查询指令
static const char *
cmd_query(struct skynet_context * context, const char * param) {
	if (param[0] == '.') {
		// 查询名字参数对应的上下文handle
		uint32_t handle = skynet_handle_findname(param+1);
		if (handle) {
			sprintf(context->result, ":%x", handle);
			return context->result;
		}
	}
	return NULL;
}

// 名字指令
// name handle
static const char *
cmd_name(struct skynet_context * context, const char * param) {
	int size = strlen(param);
	char name[size+1];
	char handle[size+1];
	sscanf(param,"%s %s",name,handle);
	if (handle[0] != ':') {
		return NULL;
	}
	// 转化为16进制handle
	// 以:开头，否则非法
	uint32_t handle_id = strtoul(handle+1, NULL, 16);
	if (handle_id == 0) {
		return NULL;
	}
	if (name[0] == '.') {
		// 为handle对应的上下文设置name
		// 以.开头，否则非法
		return skynet_handle_namehandle(handle_id, name + 1);
	} else {
		skynet_error(context, "Can't set global name %s in C", name);
	}
	return NULL;
}

// 退出指令
static const char *
cmd_exit(struct skynet_context * context, const char * param) {
	handle_exit(context, 0);
	return NULL;
}

// 传入参数，转化为handle
static uint32_t
tohandle(struct skynet_context * context, const char * param) {
	uint32_t handle = 0;
	if (param[0] == ':') {
		// 数字handle
		handle = strtoul(param+1, NULL, 16);
	} else if (param[0] == '.') {
		// 名字handle
		handle = skynet_handle_findname(param+1);
	} else {
		skynet_error(context, "Can't convert %s to handle",param);
	}

	return handle;
}

// 杀死指令
static const char *
cmd_kill(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle) {
		handle_exit(context, handle);
	}
	return NULL;
}

// 启动指令
// mod args
// snlua launcher
static const char *
cmd_launch(struct skynet_context * context, const char * param) {
	size_t sz = strlen(param);
	char tmp[sz+1];
	strcpy(tmp,param);
	char * args = tmp;
	char * mod = strsep(&args, " \t\r\n");
	args = strsep(&args, "\r\n");
	// 创建一个新的上下文
	struct skynet_context * inst = skynet_context_new(mod,args);
	if (inst == NULL) {
		return NULL;
	} else {
		// 创建成功，把上下文handle转化为字符串作为结果
		id_to_hex(context->result, inst->handle);
		return context->result;
	}
}

// 获取环境变量指令
static const char *
cmd_getenv(struct skynet_context * context, const char * param) {
	return skynet_getenv(param);
}

// 设置环境变量指令
static const char *
cmd_setenv(struct skynet_context * context, const char * param) {
	size_t sz = strlen(param);
	char key[sz+1];
	int i;
	for (i=0;param[i] != ' ' && param[i];i++) {
		key[i] = param[i];
	}
	if (param[i] == '\0')
		return NULL;

	key[i] = '\0';
	param += i+1;
	
	skynet_setenv(key,param);
	return NULL;
}

// 获得starttime
static const char *
cmd_starttime(struct skynet_context * context, const char * param) {
	uint32_t sec = skynet_starttime();
	sprintf(context->result,"%u",sec);
	return context->result;
}

// 获得endless
static const char *
cmd_endless(struct skynet_context * context, const char * param) {
	if (context->endless) {
		strcpy(context->result, "1");
		context->endless = false;
		return context->result;
	}
	return NULL;
}

// 中止所有服务
static const char *
cmd_abort(struct skynet_context * context, const char * param) {
	skynet_handle_retireall();
	return NULL;
}

// 返回/设置退出监视上下文
// 返回NULL，重新赋值
static const char *
cmd_monitor(struct skynet_context * context, const char * param) {
	uint32_t handle=0;
	if (param == NULL || param[0] == '\0') {
		if (G_NODE.monitor_exit) {
			// return current monitor serivce
			// 如果无传参，且退出监视服务不为空，返回其handle
			sprintf(context->result, ":%x", G_NODE.monitor_exit);
			return context->result;
		}
		return NULL;
	} else {
		// 转化传参为新的handle
		handle = tohandle(context, param);
	}
	//重新赋值
	G_NODE.monitor_exit = handle;
	return NULL;
}

// 返回上下文消息队列大小
static const char *
cmd_mqlen(struct skynet_context * context, const char * param) {
	int len = skynet_mq_length(context->queue);
	sprintf(context->result, "%d", len);
	return context->result;
}

// 登录上下文
static const char *
cmd_logon(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	FILE *f = NULL;
	FILE * lastf = ctx->logfile;
	if (lastf == NULL) {
		f = skynet_log_open(context, handle);
		if (f) {
			if (!ATOM_CAS_POINTER(&ctx->logfile, NULL, f)) {
				// logfile opens in other thread, close this one.
				fclose(f);
			}
		}
	}
	skynet_context_release(ctx);
	return NULL;
}

// 注销上下文
static const char *
cmd_logoff(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	FILE * f = ctx->logfile;
	if (f) {
		// logfile may close in other thread
		if (ATOM_CAS_POINTER(&ctx->logfile, f, NULL)) {
			skynet_log_close(context, f, handle);
		}
	}
	skynet_context_release(ctx);
	return NULL;
}

// 信号指令
static const char *
cmd_signal(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	param = strchr(param, ' ');
	int sig = 0;
	if (param) {
		sig = strtol(param, NULL, 0);
	}
	// NOTICE: the signal function should be thread safe.
	// 调用上下文所在模块的signal方法
	skynet_module_instance_signal(ctx->mod, ctx->instance, sig);

	skynet_context_release(ctx);
	return NULL;
}

// 指令方法集
static struct command_func cmd_funcs[] = {
	{ "TIMEOUT", cmd_timeout },
	{ "REG", cmd_reg },
	{ "QUERY", cmd_query },
	{ "NAME", cmd_name },
	{ "EXIT", cmd_exit },
	{ "KILL", cmd_kill },
	{ "LAUNCH", cmd_launch },
	{ "GETENV", cmd_getenv },
	{ "SETENV", cmd_setenv },
	{ "STARTTIME", cmd_starttime },
	{ "ENDLESS", cmd_endless },
	{ "ABORT", cmd_abort },
	{ "MONITOR", cmd_monitor },
	{ "MQLEN", cmd_mqlen },
	{ "LOGON", cmd_logon },
	{ "LOGOFF", cmd_logoff },
	{ "SIGNAL", cmd_signal },
	{ NULL, NULL },
};

// 调用相应的指令方法
const char * 
skynet_command(struct skynet_context * context, const char * cmd , const char * param) {
	struct command_func * method = &cmd_funcs[0];
	while(method->name) {
		if (strcmp(cmd, method->name) == 0) {
			return method->func(context, param);
		}
		++method;
	}

	return NULL;
}

// 过滤参数
static void
_filter_args(struct skynet_context * context, int type, int *session, void ** data, size_t * sz) {
	int needcopy = !(type & PTYPE_TAG_DONTCOPY);
	int allocsession = type & PTYPE_TAG_ALLOCSESSION;
	// 只保留低8位
	type &= 0xff;

	if (allocsession) {
		// 如果分配session，生成一个新session并赋值
		assert(*session == 0);
		*session = skynet_context_newsession(context);
	}

	if (needcopy && *data) {
		// 如果需要拷贝，重新分配内存
		char * msg = skynet_malloc(*sz+1);
		memcpy(msg, *data, *sz);
		msg[*sz] = '\0';
		*data = msg;
	}

	// type放到高8位，和sz异或后赋值给sz
	*sz |= (size_t)type << MESSAGE_TYPE_SHIFT;
}


// 发送消息
// context 当前服务上下文
// source 源服务上下文handle
// destination 目的服务上下文handle
// type 消息类型，见skynet.h
// session 消息会话
// data 数据内容
// sz 数据大小
int
skynet_send(struct skynet_context * context, uint32_t source, uint32_t destination , int type, int session, void * data, size_t sz) {
	if ((sz & MESSAGE_TYPE_MASK) != sz) {
		skynet_error(context, "The message to %x is too large", destination);
		if (type & PTYPE_TAG_DONTCOPY) {
			skynet_free(data);
		}
		return -1;
	}
	_filter_args(context, type, &session, (void **)&data, &sz);

	if (source == 0) {
		// 源地址默认为当前上下文handle
		source = context->handle;
	}

	if (destination == 0) {
		return session;
	}
	if (skynet_harbor_message_isremote(destination)) {
		// 如果是远程地址，包装成remote_message，发送给harbor节点
		struct remote_message * rmsg = skynet_malloc(sizeof(*rmsg));
		rmsg->destination.handle = destination;
		rmsg->message = data;
		rmsg->sz = sz;
		skynet_harbor_send(rmsg, source, session);
	} else {
		// 如果不是远程地址，包装成skynet_message，压入目的地址上下文的消息队列
		struct skynet_message smsg;
		smsg.source = source;
		smsg.session = session;
		smsg.data = data;
		smsg.sz = sz;

		if (skynet_context_push(destination, &smsg)) {
			skynet_free(data);
			return -1;
		}
	}
	return session;
}

// 发送消息
// 名字地址，查询handle
int
skynet_sendname(struct skynet_context * context, uint32_t source, const char * addr , int type, int session, void * data, size_t sz) {
	if (source == 0) {
		source = context->handle;
	}
	uint32_t des = 0;
	if (addr[0] == ':') {
		des = strtoul(addr+1, NULL, 16);
	} else if (addr[0] == '.') {
		des = skynet_handle_findname(addr + 1);
		if (des == 0) {
			if (type & PTYPE_TAG_DONTCOPY) {
				skynet_free(data);
			}
			return -1;
		}
	} else {
		_filter_args(context, type, &session, (void **)&data, &sz);

		struct remote_message * rmsg = skynet_malloc(sizeof(*rmsg));
		copy_name(rmsg->destination.name, addr);
		rmsg->destination.handle = 0;
		rmsg->message = data;
		rmsg->sz = sz;

		skynet_harbor_send(rmsg, source, session);
		return session;
	}

	return skynet_send(context, source, des, type, session, data, sz);
}

// 返回上下文handle
uint32_t 
skynet_context_handle(struct skynet_context *ctx) {
	return ctx->handle;
}

// 注册上下文回调
void 
skynet_callback(struct skynet_context * context, void *ud, skynet_cb cb) {
	context->cb = cb; 	// 回调函数
	context->cb_ud = ud;	// 执行回调函数的模块对象
}

// 上下文压入一条消息
void
skynet_context_send(struct skynet_context * ctx, void * msg, size_t sz, uint32_t source, int type, int session) {
	struct skynet_message smsg;
	smsg.source = source;
	smsg.session = session;
	smsg.data = msg;
	smsg.sz = sz | (size_t)type << MESSAGE_TYPE_SHIFT;

	skynet_mq_push(ctx->queue, &smsg);
}

// skynet全局变量初始化
void 
skynet_globalinit(void) {
	G_NODE.total = 0;
	G_NODE.monitor_exit = 0;
	G_NODE.init = 1;
	if (pthread_key_create(&G_NODE.handle_key, NULL)) {
		fprintf(stderr, "pthread_key_create failed");
		exit(1);
	}
	// set mainthread's key
	skynet_initthread(THREAD_MAIN);
}

// 全局退出删除私有线程变量
void 
skynet_globalexit(void) {
	pthread_key_delete(G_NODE.handle_key);
}

// 初始化私有线程变量
void
skynet_initthread(int m) {
	uintptr_t v = (uint32_t)(-m);
	pthread_setspecific(G_NODE.handle_key, (void *)v);
}

