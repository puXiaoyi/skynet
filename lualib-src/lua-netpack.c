#include "skynet_malloc.h"

#include "skynet_socket.h"

#include <lua.h>
#include <lauxlib.h>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define QUEUESIZE 1024	// ��ʼ��Ϣ����������
#define HASHSIZE 4096	// ��ʼδ������鳤��
#define SMALLSTRING 2048

// �������ͺ�
#define TYPE_DATA 1		// ������д��
#define TYPE_MORE 2		// �и�������
#define TYPE_ERROR 3	// ����
#define TYPE_OPEN 4		// ��
#define TYPE_CLOSE 5	// �ر�
#define TYPE_WARNING 6	// ����

/*
	Each package is uint16 + data , uint16 (serialized in big-endian) is the number of bytes comprising the data .
 */
// header(2 bytes) + data
// ��Ϣ���ṹ
struct netpack {
	int id;				// socket��Ϣ��fd
	int size;			// socket��Ϣ�ĳ���
	void * buffer;		// socket��Ϣ�Ļ���������
};

// δ��ɵ���Ϣ���ṹ
struct uncomplete {
	struct netpack pack;		// ��Ϣ��
	struct uncomplete * next;	// ��һ��ָ��
	int read;					// �Ѷ�����
	int header;					// 
};

// ��Ϣ�����нṹ
struct queue {
	int cap;							// ����					
	int head;							// ͷ����
	int tail;							// β����
	struct uncomplete * hash[HASHSIZE]; // δ�������
	struct netpack queue[QUEUESIZE];	// ����������
};

// ���δ�������
static void
clear_list(struct uncomplete * uc) {
	while (uc) {
		skynet_free(uc->pack.buffer);
		void * tmp = uc;
		uc = uc->next;
		skynet_free(tmp);
	}
}

// �����Ϣ������
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
		// �����Ʋ�Ӱ��ȡ��ֵ��Ϊ�˷����������������
		q->tail += q->cap;
	}
	for (i=q->head;i<q->tail;i++) {
		struct netpack *np = &q->queue[i % q->cap];
		skynet_free(np->buffer);
	}
	q->head = q->tail = 0;

	return 0;
}

// ����fd��hashֵ
static inline int
hash_fd(int fd) {
	int a = fd >> 24;
	int b = fd >> 12;
	int c = fd;
	return (int)(((uint32_t)(a + b + c)) % HASHSIZE);
}

// ����δ�����Ϣ��
static struct uncomplete *
find_uncomplete(struct queue *q, int fd) {
	if (q == NULL)
		return NULL;
	// ����q->hash�й�ϣ����
	int h = hash_fd(fd);
	struct uncomplete * uc = q->hash[h];
	if (uc == NULL)
		return NULL;
	if (uc->pack.id == fd) {
		q->hash[h] = uc->next;
		return uc;
	}
	// ����������ѭ������
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

// ������Ϣ�����У����Ϊ���򴴽�����ʼ��
static struct queue *
get_queue(lua_State *L) {
	struct queue *q = lua_touserdata(L,1);
	if (q == NULL) {
		// ��ȫ�û����ݣ�ָ���Ƿ���һ��ָ����С���ڴ�飬���ڴ���ַѹջ�����������ַ�����������������ʹ������ڴ档
		// �����û����ݣ������ֻ��void*ָ�룬��һ�����ֿ����໥�Ƚϣ����ÿ��ⴴ���ڴ�ռ�
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

// ������Ϣ������
static void
expand_queue(lua_State *L, struct queue *q) {
	// �����µ���ȫ�û�����	
	struct queue *nq = lua_newuserdata(L, sizeof(struct queue) + q->cap * sizeof(struct netpack));
	nq->cap = q->cap + QUEUESIZE;
	// ������head��tail����
	nq->head = 0;
	nq->tail = q->cap;
	// Ǩ�����ݵ��µĶ���
	memcpy(nq->hash, q->hash, sizeof(nq->hash));
	memset(q->hash, 0, sizeof(q->hash));
	int i;
	for (i=0;i<q->cap;i++) {
		int idx = (q->head + i) % q->cap;
		nq->queue[i] = q->queue[idx];
	}
	q->head = q->tail = 0;
	// ��ջ��Ԫ�ط��õ�����λ�ö����ƶ�����Ԫ�� ����˸������Ǹ�λ�ô���ֵ����Ȼ��ջ��Ԫ�ص���
	// ����ջ������Ϊnq
	lua_replace(L,1);
}

// ѹ����Ϣ������
static void
push_data(lua_State *L, int fd, void *buffer, int size, int clone) {
	if (clone) {
		// �����Ҫ�����������·����ڴ�ռ�
		void * tmp = skynet_malloc(size);
		memcpy(tmp, buffer, size);
		buffer = tmp;
	}
	// ��ȡ��Ϣ������
	struct queue *q = get_queue(L);
	// ���ݷ������β��
	struct netpack *np = &q->queue[q->tail];
	if (++q->tail >= q->cap)
		// ����
		q->tail -= q->cap;
	np->id = fd;
	np->buffer = buffer;
	np->size = size;
	if (q->head == q->tail) {
		// ����������������
		expand_queue(L, q);
	}
}

// ����δ���
static struct uncomplete *
save_uncomplete(lua_State *L, int fd) {
	struct queue *q = get_queue(L);
	int h = hash_fd(fd);
	// ����һ��δ���ʵ������������͹�ϣ������
	struct uncomplete * uc = skynet_malloc(sizeof(struct uncomplete));
	memset(uc, 0, sizeof(*uc));
	uc->next = q->hash[h];
	uc->pack.id = fd;
	q->hash[h] = uc;

	return uc;
}

// ����2�ֽ�ͷ���㳤��
static inline int
read_size(uint8_t * buffer) {
	int r = (int)buffer[0] << 8 | (int)buffer[1];
	return r;
}

// �����������
static void
push_more(lua_State *L, int fd, uint8_t *buffer, int size) {
	if (size == 1) {
		// �������ֻ��1����Ϊ�涨ͷ��2�ֽڣ���ͷ������
		// ����δ���
		struct uncomplete * uc = save_uncomplete(L, fd);
		// ��Ϊֻ��һ���ֽڣ�ȫ�����ǰ�ͷ
		// read��ֵΪ-1����Ϊ�����ʶ
		uc->read = -1;
		uc->header = *buffer;
		return;
	}
	// �������ݰ��ĳ��ȣ�������2�ֽڰ�ͷ
	int pack_size = read_size(buffer);
	buffer += 2;
	size -= 2;

	if (size < pack_size) {
		// ��������
		// ����δ���
		struct uncomplete * uc = save_uncomplete(L, fd);
		// ��¼�Ѷ���Ϣ��δ��ɽṹ
		uc->read = size;							// �Ѷ�����
		uc->pack.size = pack_size;
		uc->pack.buffer = skynet_malloc(pack_size);	// ʵ�ʳ���
		memcpy(uc->pack.buffer, buffer, size);		// ��������
		return;
	}
	// ������
	// ѹ�����ݰ�����
	push_data(L, fd, buffer, pack_size, 1);

	buffer += pack_size;
	size -= pack_size;
	if (size > 0) {
		// ������������ȴ��ڰ�ͷ���ȣ�˵��һ��������������ݰ����ݹ���ö�ȡ��������
		push_more(L, fd, buffer, size);
	}
}

// �ر�δ��ɵ�
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
		// �����δ���
		// fill uncomplete
		if (uc->read < 0) {
			// read size
			// ���readΪ-1��bufferֻ������1�ֽڵ�ͷ����buffer[0]
			assert(uc->read == -1);
			// �ٶ�һ���ֽ�buffer[1]
			int pack_size = *buffer;
			// buffer[0] << 8 | buffer[1] �������ʵ�����ݰ�����
			pack_size |= uc->header << 8 ;
			// ƫ��׼����ȡͷ����İ�����
			++buffer;
			--size;
			// ����δ��ɽṹ������������ʵ�����ݰ������Լ����仺�����ռ�
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			uc->read = 0;
		}
		// ��Ҫ������ȡ�����ݳ���
		int need = uc->pack.size - uc->read;
		if (size < need) {
			// ��������Ȼ��ǲ���������δ��ɽṹ����
			memcpy(uc->pack.buffer + uc->read, buffer, size);
			uc->read += size;
			int h = hash_fd(fd);
			uc->next = q->hash[h];
			q->hash[h] = uc;
			return 1;
		}
		// �������һ���Զ���
		memcpy(uc->pack.buffer + uc->read, buffer, need);
		buffer += need;
		size -= need;
		if (size == 0) {
			// ����պö��꣬û��ʣ������
			// ѹ����ֵ "data"
			// ѹ����Ϣ fd
			// ѹ�뻺�������� msg
			// ѹ�뻺������С sz
			// ���� queue, data, fd, msg, sz ��5������
			// dispatch_msg(fd, msg, sz) �� lualib/snax/gateserver.lua
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			lua_pushlightuserdata(L, uc->pack.buffer);
			lua_pushinteger(L, uc->pack.size);
			skynet_free(uc);
			return 5;
		}
		// more data
		// �������󣬻���ʣ������
		// �ȶ���һ����
		push_data(L, fd, uc->pack.buffer, uc->pack.size, 0);
		skynet_free(uc);
		// �ٶ���ʣ������
		push_more(L, fd, buffer, size);
		// ѹ����ֵ "more"
		// ���� queue, more ��2������
		// dispatch_queue() �� lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	} else {
		// ���û��δ���
		if (size == 1) {
			// ֻ��һ�ֽڰ�ͷ��ͬpush_more
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = -1;
			uc->header = *buffer;
			return 1;
		}
		// �������ݰ�����
		int pack_size = read_size(buffer);
		buffer+=2;
		size-=2;

		if (size < pack_size) {
			// ��������Ȳ���������δ��ɽṹ����
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = size;
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			memcpy(uc->pack.buffer, buffer, size);
			return 1;
		}
		if (size == pack_size) {
			// just one package
			// ѹ����ֵ "data"
			// ѹ����Ϣ fd
			// ѹ�뻺�������� msg
			// ѹ�뻺������С sz
			// ���� queue, data, fd, msg, sz ��5������
			// dispatch_msg(fd, msg, sz) �� lualib/snax/gateserver.lua
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			void * result = skynet_malloc(pack_size);
			memcpy(result, buffer, size);
			lua_pushlightuserdata(L, result);
			lua_pushinteger(L, size);
			return 5;
		}
		// more data
		// �������󣬻���ʣ������
		// �ȶ���һ����
		push_data(L, fd, buffer, pack_size, 1);
		buffer += pack_size;
		size -= pack_size;
		// �ٶ���ʣ������
		push_more(L, fd, buffer, size);
		// ѹ����ֵ "more"
		// ���� queue, more ��2������
		// dispatch_queue() �� lualib/snax/gateserver.lua
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
	// ��ȡ�ڶ�������ֵ��skynet_socket_message
	struct skynet_socket_message *message = lua_touserdata(L,2);
	// ��ȡ����������ֵ�����ݳ���
	int size = luaL_checkinteger(L,3);
	char * buffer = message->buffer;
	// SOCKET_OPEN SOCKET_ACCEPT SOCKET_ERROR û�а�data����д�뻺���������Ƿ���֮����ڴ�ռ�
	// ����� skynet_socket �е� forward_message ����
	if (buffer == NULL) {
		// ������������
		buffer = (char *)(message+1);
		size -= sizeof(*message);
	} else {
		size = -1;
	}

	// ����ջ��Ϊ1
	// �����˵�һ������queue�������������������
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
		// ѹ����ֵ "close"
		// ѹ����Ϣ fd
		// ���� queue, close, fd ��3������
		// MSG.close(fd) �� lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_CLOSE));
		lua_pushinteger(L, message->id);
		return 3;
	case SKYNET_SOCKET_TYPE_ACCEPT:
		// ѹ����ֵ "open"
		// ѹ����Ϣ fd
		// ѹ������ buffer
		// ���� queue, open, fd, msg ��4������
		// MSG.open(fd, msg) �� lualib/snax/gateserver.lua
		lua_pushvalue(L, lua_upvalueindex(TYPE_OPEN));
		// ignore listen id (message->id);
		lua_pushinteger(L, message->ud);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_ERROR:
		// no more data in fd (message->id)
		// ѹ����ֵ "error"
		// ѹ����Ϣ fd
		// ѹ������ buffer
		// ���� queue, error, fd, msg ��4������
		// MSG.error(fd, msg) �� lualib/snax/gateserver.lua
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
		lua_pushinteger(L, message->id);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_WARNING:
		// ѹ����ֵ "warning"
		// ѹ����Ϣ fd
		// ѹ�볤�� sz
		// ���� queue, error, fd, sz ��4������
		// MSG.error(fd, sz) �� lualib/snax/gateserver.lua
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

// lenд��2�ֽ�ͷ
static inline void
write_size(uint8_t * buffer, int len) {
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
}

// netpack.pack
// ��Ϣ���������2�ֽڵ���Ϣͷ
// ����msg, sz
static int
lpack(lua_State *L) {
	size_t len;
	const char * ptr = tolstring(L, &len, 1);
	if (len >= 0x10000) {
		// ��Ϣ���Ȳ�����64K
		// Լ����ÿ����Ϣ������2�ֽ�ͷ
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}

	// �����ڴ洴��������
	uint8_t * buffer = skynet_malloc(len + 2);
	// ����д�뻺����ͷ��
	write_size(buffer, len);
	// ����д�뻺����β��
	memcpy(buffer+2, ptr, len);

	// �ѻ��������ݺͳ���ѹ��ջ����Ϊlua����ֵ
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
	// ����һ���µı������б� l �еĺ���ע���ȥ��
	luaL_newlib(L,l);

	// the order is same with macros : TYPE_* (defined top)
	// ���Զ������ͺ��˳���һЩ������ѹ��ջ��
	lua_pushliteral(L, "data");
	lua_pushliteral(L, "more");
	lua_pushliteral(L, "error");
	lua_pushliteral(L, "open");
	lua_pushliteral(L, "close");
	lua_pushliteral(L, "warning");

	// ��һ���µ� C �հ�ѹջ
	// fn = lfilter
	// n = 6
	// ��ѹ��ջ��6����������Ϊ�հ�����lfilter�ıհ���ֵ��ͬʱ��ջ�е���
	lua_pushcclosure(L, lfilter, 6);
	// ��ջ���հ�����lfilter����Ϊnewlibtable��filter��Ӧֵ
	lua_setfield(L, -2, "filter");

	return 1;
}
