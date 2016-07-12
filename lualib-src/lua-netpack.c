#include "skynet_malloc.h"

#include "skynet_socket.h"

#include <lua.h>
#include <lauxlib.h>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define QUEUESIZE 1024	// 初始消息包队列容量
#define HASHSIZE 4096	// 初始未完成数组长度
#define SMALLSTRING 2048

// 定义类型宏
#define TYPE_DATA 1		// 有数据写入
#define TYPE_MORE 2		// 有更多数据
#define TYPE_ERROR 3	// 出错
#define TYPE_OPEN 4		// 打开
#define TYPE_CLOSE 5	// 关闭
#define TYPE_WARNING 6	// 警告

/*
	Each package is uint16 + data , uint16 (serialized in big-endian) is the number of bytes comprising the data .
 */
// header(2 bytes) + data
// 消息包结构
struct netpack {
	int id;				// socket消息的fd
	int size;			// socket消息的长度
	void * buffer;		// socket消息的缓冲区内容
};

// 未完成的消息包结构
struct uncomplete {
	struct netpack pack;		// 消息包
	struct uncomplete * next;	// 下一个指针
	int read;					// 已读长度
	int header;					// 
};

// 消息包队列结构
struct queue {
	int cap;							// 容量					
	int head;							// 头索引
	int tail;							// 尾索引
	struct uncomplete * hash[HASHSIZE]; // 未完成数组
	struct netpack queue[QUEUESIZE];	// 缓冲区队列
};

// 清空未完成链表
static void
clear_list(struct uncomplete * uc) {
	while (uc) {
		skynet_free(uc->pack.buffer);
		void * tmp = uc;
		uc = uc->next;
		skynet_free(tmp);
	}
}

// 清空消息包队列
static int
lclear(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL) {
		return 0;
	}
	int i;
	for (i=0;i<HASHSIZE;i++) {
		clear_list(q->hash[i]);
		q->hash[i] = NULL;
	}
	if (q->head > q->tail) {
		// 不回绕不影响取余值，为了方便下面遍历缓冲区
		q->tail += q->cap;
	}
	for (i=q->head;i<q->tail;i++) {
		struct netpack *np = &q->queue[i % q->cap];
		skynet_free(np->buffer);
	}
	q->head = q->tail = 0;

	return 0;
}

// 计算fd的hash值
static inline int
hash_fd(int fd) {
	int a = fd >> 24;
	int b = fd >> 12;
	int c = fd;
	return (int)(((uint32_t)(a + b + c)) % HASHSIZE);
}

// 查找未完成消息包
static struct uncomplete *
find_uncomplete(struct queue *q, int fd) {
	if (q == NULL)
		return NULL;
	// 先在q->hash中哈希查找
	int h = hash_fd(fd);
	struct uncomplete * uc = q->hash[h];
	if (uc == NULL)
		return NULL;
	if (uc->pack.id == fd) {
		q->hash[h] = uc->next;
		return uc;
	}
	// 再在链表中循环查找
	struct uncomplete * last = uc;
	while (last->next) {
		uc = last->next;
		if (uc->pack.id == fd) {
			last->next = uc->next;
			return uc;
		}
		last = uc;
	}
	return NULL;
}

// 返回消息包队列，如果为空则创建并初始化
static struct queue *
get_queue(lua_State *L) {
	struct queue *q = lua_touserdata(L,1);
	if (q == NULL) {
		// 完全用户数据，指的是分配一块指定大小的内存块，把内存块地址压栈并返回这个地址，宿主程序可以任意使用这块内存。
		// 轻量用户数据，保存的只是void*指针，是一个数字可以相互比较，不用刻意创建内存空间
		q = lua_newuserdata(L, sizeof(struct queue));
		q->cap = QUEUESIZE;
		q->head = 0;
		q->tail = 0;
		int i;
		for (i=0;i<HASHSIZE;i++) {
			q->hash[i] = NULL;
		}
		lua_replace(L, 1);
	}
	return q;
}

// 扩容消息包队列
static void
expand_queue(lua_State *L, struct queue *q) {
	// 创建新的完全用户数据	
	struct queue *nq = lua_newuserdata(L, sizeof(struct queue) + q->cap * sizeof(struct netpack));
	nq->cap = q->cap + QUEUESIZE;
	// 重置了head和tail索引
	nq->head = 0;
	nq->tail = q->cap;
	// 迁移数据到新的队列
	memcpy(nq->hash, q->hash, sizeof(nq->hash));
	memset(q->hash, 0, sizeof(q->hash));
	int i;
	for (i=0;i<q->cap;i++) {
		int idx = (q->head + i) % q->cap;
		nq->queue[i] = q->queue[idx];
	}
	q->head = q->tail = 0;
	// 把栈顶元素放置到给定位置而不移动其它元素 （因此覆盖了那个位置处的值），然后将栈顶元素弹出
	// 更换栈上数据为nq
	lua_replace(L,1);
}

// 压入消息包队列
static void
push_data(lua_State *L, int fd, void *buffer, int size, int clone) {
	if (clone) {
		// 如果需要拷贝，则重新分配内存空间
		void * tmp = skynet_malloc(size);
		memcpy(tmp, buffer, size);
		buffer = tmp;
	}
	// 获取消息包队列
	struct queue *q = get_queue(L);
	// 数据放入队列尾部
	struct netpack *np = &q->queue[q->tail];
	if (++q->tail >= q->cap)
		// 回绕
		q->tail -= q->cap;
	np->id = fd;
	np->buffer = buffer;
	np->size = size;
	if (q->head == q->tail) {
		// 队列满，进行扩容
		expand_queue(L, q);
	}
}

// 保存未完成
static struct uncomplete *
save_uncomplete(lua_State *L, int fd) {
	struct queue *q = get_queue(L);
	int h = hash_fd(fd);
	// 创建一个未完成实例，放入链表和哈希数组中
	struct uncomplete * uc = skynet_malloc(sizeof(struct uncomplete));
	memset(uc, 0, sizeof(*uc));
	uc->next = q->hash[h];
	uc->pack.id = fd;
	q->hash[h] = uc;

	return uc;
}

// 读入2字节头计算长度
static inline int
read_size(uint8_t * buffer) {
	int r = (int)buffer[0] << 8 | (int)buffer[1];
	return r;
}

// 读入更多数据
static void
push_more(lua_State *L, int fd, uint8_t *buffer, int size) {
	if (size == 1) {
		// 如果长度只有1，因为规定头部2字节，包头不完整
		// 保存未完成
		struct uncomplete * uc = save_uncomplete(L, fd);
		// 因为只有一个字节，全部都是包头
		// read赋值为-1，作为特殊标识
		uc->read = -1;
		uc->header = *buffer;
		return;
	}
	// 计算数据包的长度，不包含2字节包头
	int pack_size = read_size(buffer);
	buffer += 2;
	size -= 2;

	if (size < pack_size) {
		// 包不完整
		// 保存未完成
		struct uncomplete * uc = save_uncomplete(L, fd);
		// 记录已读信息到未完成结构
		uc->read = size;							// 已读长度
		uc->pack.size = pack_size;
		uc->pack.buffer = skynet_malloc(pack_size);	// 实际长度
		memcpy(uc->pack.buffer, buffer, size);		// 复制数据
		return;
	}
	// 包完整
	// 压入数据包队列
	push_data(L, fd, buffer, pack_size, 1);

	buffer += pack_size;
	size -= pack_size;
	if (size > 0) {
		// 如果缓冲区长度大于包头长度，说明一个缓冲区多个数据包，递归调用读取更多数据
		push_more(L, fd, buffer, size);
	}
}

// 关闭未完成的
static void
close_uncomplete(lua_State *L, int fd) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		skynet_free(uc->pack.buffer);
		skynet_free(uc);
	}
}

static int
filter_data_(lua_State *L, int fd, uint8_t * buffer, int size) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		// 如果有未完成
		// fill uncomplete
		if (uc->read < 0) {
			// read size
			// 如果read为-1，buffer只保存了1字节的头，即buffer[0]
			assert(uc->read == -1);
			// 再读一个字节buffer[1]
			int pack_size = *buffer;
			// buffer[0] << 8 | buffer[1] 计算出真实的数据包长度
			pack_size |= uc->header << 8 ;
			// 偏移准备读取头后面的包数据
			++buffer;
			--size;
			// 更新未完成结构变量，包括真实的数据包长度以及分配缓冲区空间
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			uc->read = 0;
		}
		// 需要继续读取的数据长度
		int need = uc->pack.size - uc->read;
		if (size < need) {
			// 如果包长度还是不够，更新未完成结构变量
			memcpy(uc->pack.buffer + uc->read, buffer, size);
			uc->read += size;
			int h = hash_fd(fd);
			uc->next = q->hash[h];
			q->hash[h] = uc;
			return 1;
		}
		// 如果可以一次性读完
		memcpy(uc->pack.buffer + uc->read, buffer, need);
		buffer += need;
		size -= need;
		if (size == 0) {
			// 如果刚好读完，没有剩余数据
			// 压入上值 "data"
			// 压入消息 fd
			// 压入缓冲区数据 msg
			// 压入缓冲区大小 sz
			// 返回 queue, data, fd, msg, sz 这5个参数
			// dispatch_msg(fd, msg, sz) 见 lualib/snax/gateserver.lua
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			lua_pushlightuserdata(L, uc->pack.buffer);
			lua_pushinteger(L, uc->pack.size);
			skynet_free(uc);
			return 5;
		}
		// more data
		// 如果读完后，还有剩余数据
		// 先读入一个包
		push_data(L, fd, uc->pack.buffer, uc->pack.size, 0);
		skynet_free(uc);
		// 再读入剩余数据
		push_more(L, fd, buffer, size);
		// 压入上值 "more"
		// 返回 queue, more 这2个参数
		// dispatch_queue() 见 lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	} else {
		// 如果没有未完成
		if (size == 1) {
			// 只有一字节包头，同push_more
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = -1;
			uc->header = *buffer;
			return 1;
		}
		// 计算数据包长度
		int pack_size = read_size(buffer);
		buffer+=2;
		size-=2;

		if (size < pack_size) {
			// 如果包长度不够，保存未完成结构变量
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = size;
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			memcpy(uc->pack.buffer, buffer, size);
			return 1;
		}
		if (size == pack_size) {
			// just one package
			// 压入上值 "data"
			// 压入消息 fd
			// 压入缓冲区数据 msg
			// 压入缓冲区大小 sz
			// 返回 queue, data, fd, msg, sz 这5个参数
			// dispatch_msg(fd, msg, sz) 见 lualib/snax/gateserver.lua
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			void * result = skynet_malloc(pack_size);
			memcpy(result, buffer, size);
			lua_pushlightuserdata(L, result);
			lua_pushinteger(L, size);
			return 5;
		}
		// more data
		// 如果读完后，还有剩余数据
		// 先读入一个包
		push_data(L, fd, buffer, pack_size, 1);
		buffer += pack_size;
		size -= pack_size;
		// 再读入剩余数据
		push_more(L, fd, buffer, size);
		// 压入上值 "more"
		// 返回 queue, more 这2个参数
		// dispatch_queue() 见 lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	}
}

static inline int
filter_data(lua_State *L, int fd, uint8_t * buffer, int size) {
	int ret = filter_data_(L, fd, buffer, size);
	// buffer is the data of socket message, it malloc at socket_server.c : function forward_message .
	// it should be free before return,
	skynet_free(buffer);
	return ret;
}

static void
pushstring(lua_State *L, const char * msg, int size) {
	if (msg) {
		lua_pushlstring(L, msg, size);
	} else {
		lua_pushliteral(L, "");
	}
}

/*
	userdata queue
	lightuserdata msg
	integer size
	return
		userdata queue
		integer type
		integer fd
		string msg | lightuserdata/integer
 */
 // netpack.filter(queue, msg, sz)
 // return queue, type, fd, msg, sz
static int
lfilter(lua_State *L) {
	// 获取第二个参数值，skynet_socket_message
	struct skynet_socket_message *message = lua_touserdata(L,2);
	// 获取第三个参数值，数据长度
	int size = luaL_checkinteger(L,3);
	char * buffer = message->buffer;
	// SOCKET_OPEN SOCKET_ACCEPT SOCKET_ERROR 没有把data数据写入缓冲区，而是放入之后的内存空间
	// 具体见 skynet_socket 中的 forward_message 方法
	if (buffer == NULL) {
		// 如果是填充数据
		buffer = (char *)(message+1);
		size -= sizeof(*message);
	} else {
		size = -1;
	}

	// 设置栈顶为1
	// 保留了第一个参数queue，清空了其他两个参数
	lua_settop(L, 1);

	switch(message->type) {
	case SKYNET_SOCKET_TYPE_DATA:
		// ignore listen id (message->id)
		assert(size == -1);	// never padding string
		return filter_data(L, message->id, (uint8_t *)buffer, message->ud);
	case SKYNET_SOCKET_TYPE_CONNECT:
		// ignore listen fd connect
		return 1;
	case SKYNET_SOCKET_TYPE_CLOSE:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		// 压入上值 "close"
		// 压入消息 fd
		// 返回 queue, close, fd 这3个参数
		// MSG.close(fd) 见 lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_CLOSE));
		lua_pushinteger(L, message->id);
		return 3;
	case SKYNET_SOCKET_TYPE_ACCEPT:
		// 压入上值 "open"
		// 压入消息 fd
		// 压入数据 buffer
		// 返回 queue, open, fd, msg 这4个参数
		// MSG.open(fd, msg) 见 lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_OPEN));
		// ignore listen id (message->id);
		lua_pushinteger(L, message->ud);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_ERROR:
		// no more data in fd (message->id)
		// 压入上值 "error"
		// 压入消息 fd
		// 压入数据 buffer
		// 返回 queue, error, fd, msg 这4个参数
		// MSG.error(fd, msg) 见 lualib/snax/gateserver.lua
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
		lua_pushinteger(L, message->id);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_WARNING:
		// 压入上值 "warning"
		// 压入消息 fd
		// 压入长度 sz
		// 返回 queue, error, fd, sz 这4个参数
		// MSG.error(fd, sz) 见 lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_WARNING));
		lua_pushinteger(L, message->id);
		lua_pushinteger(L, message->ud);
		return 4;
	default:
		// never get here
		return 1;
	}
}

/*
	userdata queue
	return
		integer fd
		lightuserdata msg
		integer size
 */
static int
lpop(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL || q->head == q->tail)
		return 0;
	struct netpack *np = &q->queue[q->head];
	if (++q->head >= q->cap) {
		q->head = 0;
	}
	lua_pushinteger(L, np->id);
	lua_pushlightuserdata(L, np->buffer);
	lua_pushinteger(L, np->size);

	return 3;
}

/*
	string msg | lightuserdata/integer

	lightuserdata/integer
 */

static const char *
tolstring(lua_State *L, size_t *sz, int index) {
	const char * ptr;
	if (lua_isuserdata(L,index)) {
		// msg sz
		ptr = (const char *)lua_touserdata(L,index);
		*sz = (size_t)luaL_checkinteger(L, index+1);
	} else {
		// msg
		ptr = luaL_checklstring(L, index, sz);
	}
	return ptr;
}

// len写入2字节头
static inline void
write_size(uint8_t * buffer, int len) {
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
}

// netpack.pack
// 消息打包，加上2字节的消息头
// 返回msg, sz
static int
lpack(lua_State *L) {
	size_t len;
	const char * ptr = tolstring(L, &len, 1);
	if (len >= 0x10000) {
		// 消息长度不超过64K
		// 约定了每个消息包都是2字节头
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}

	// 分配内存创建缓冲区
	uint8_t * buffer = skynet_malloc(len + 2);
	// 长度写入缓冲区头部
	write_size(buffer, len);
	// 数据写入缓冲区尾部
	memcpy(buffer+2, ptr, len);

	// 把缓冲区数据和长度压入栈中作为lua返回值
	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, len + 2);

	return 2;
}

// netpack.tostring(msg, sz)
static int
ltostring(lua_State *L) {
	void * ptr = lua_touserdata(L, 1);
	int size = luaL_checkinteger(L, 2);
	if (ptr == NULL) {
		lua_pushliteral(L, "");
	} else {
		lua_pushlstring(L, (const char *)ptr, size);
		skynet_free(ptr);
	}
	return 1;
}

// require "netpack"
int
luaopen_netpack(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pop", lpop },
		{ "pack", lpack },
		{ "clear", lclear },
		{ "tostring", ltostring },
		{ NULL, NULL },
	};
	// 创建一张新的表，并把列表 l 中的函数注册进去。
	luaL_newlib(L,l);

	// the order is same with macros : TYPE_* (defined top)
	// 按自定义类型宏的顺序把一些字面量压入栈中
	lua_pushliteral(L, "data");
	lua_pushliteral(L, "more");
	lua_pushliteral(L, "error");
	lua_pushliteral(L, "open");
	lua_pushliteral(L, "close");
	lua_pushliteral(L, "warning");

	// 把一个新的 C 闭包压栈
	// fn = lfilter
	// n = 6
	// 把压入栈的6个字面量作为闭包函数lfilter的闭包上值，同时从栈中弹出
	lua_pushcclosure(L, lfilter, 6);
	// 把栈顶闭包函数lfilter设置为newlibtable的filter对应值
	lua_setfield(L, -2, "filter");

	return 1;
}
