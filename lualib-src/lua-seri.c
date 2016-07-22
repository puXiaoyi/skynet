/*
	modify from https://github.com/cloudwu/lua-serialize
 */

#include "skynet_malloc.h"

#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define TYPE_NIL 0
#define TYPE_BOOLEAN 1
// hibits 0 false 1 true
#define TYPE_NUMBER 2
// hibits 0 : 0 , 1: byte, 2:word, 4: dword, 6: qword, 8 : double
/*
	数字型，通过 TYPE_NUMBER 和 子类型 计算复合类型
	COMBINE_TYPE(t,v) ((t) | (v) << 3)
	
	TYPE_NUMBER_ZERO	0
	TYPE_NUMBER_BYTE	8位无符号整数
	TYPE_NUMBER_WORD 	16位无符号整数
	TYPE_NUMBER_DWORD	32位整数
	TYPE_NUMBER_QWORD	64位整数
	TYPE_NUMBER_REAL 	双精度浮点数
*/
#define TYPE_NUMBER_ZERO 0
#define TYPE_NUMBER_BYTE 1
#define TYPE_NUMBER_WORD 2
#define TYPE_NUMBER_DWORD 4
#define TYPE_NUMBER_QWORD 6
#define TYPE_NUMBER_REAL 8

#define TYPE_USERDATA 3
/*
	字符串类型，具体不同见wb_string
*/
#define TYPE_SHORT_STRING 4	
// hibits 0~31 : len
#define TYPE_LONG_STRING 5
#define TYPE_TABLE 6

#define MAX_COOKIE 32
#define COMBINE_TYPE(t,v) ((t) | (v) << 3)

#define BLOCK_SIZE 128
#define MAX_DEPTH 32

// 写缓冲区块
struct block {
	struct block * next;		// 下一块指针
	char buffer[BLOCK_SIZE];	// 缓冲区数据
};

// 写缓冲区链表
struct write_block {
	struct block * head;		// 头部块指针
	struct block * current;		// 当前块指针
	int len;					// 已读数据长度
	int ptr;					// 指针偏移大小
};

// 读缓冲区块
struct read_block {
	char * buffer;				// 数据缓冲区
	int len;					// 已读数据长度
	int ptr;					// 指针偏移大小
};

// 分配写缓冲区块内存
inline static struct block *
blk_alloc(void) {
	// 除了第一个缓冲区块在栈上，其他手动分配堆内存
	struct block *b = skynet_malloc(sizeof(struct block));
	b->next = NULL;
	return b;
}

// 加入写缓冲区
inline static void
wb_push(struct write_block *b, const void *buf, int sz) {
	const char * buffer = buf;
	if (b->ptr == BLOCK_SIZE) {
_again:
		b->current = b->current->next = blk_alloc();
		b->ptr = 0;
	}
	if (b->ptr <= BLOCK_SIZE - sz) {
		// 如果当前缓冲区足够写入数据
		memcpy(b->current->buffer + b->ptr, buffer, sz);
		b->ptr+=sz;
		b->len+=sz;
	} else {
		// 如果当前缓冲区不够写入数据
		int copy = BLOCK_SIZE - b->ptr;
		// 把当前缓冲区写满，继续往下一个缓冲区写
		memcpy(b->current->buffer + b->ptr, buffer, copy);
		buffer += copy;
		b->len += copy;
		sz -= copy;
		goto _again;
	}
}

// 初始化写缓冲区链表
static void
wb_init(struct write_block *wb , struct block *b) {
	wb->head = b;
	assert(b->next == NULL);
	wb->len = 0;
	wb->current = wb->head;
	wb->ptr = 0;
}

// 释放写缓冲区链表
static void
wb_free(struct write_block *wb) {
	struct block *blk = wb->head;
	// 第一个缓冲区块在栈上
	blk = blk->next;	// the first block is on stack
	while (blk) {
		struct block * next = blk->next;
		skynet_free(blk);
		blk = next;
	}
	wb->head = NULL;
	wb->current = NULL;
	wb->ptr = 0;
	wb->len = 0;
}

// 初始化读缓冲区块
static void
rball_init(struct read_block * rb, char * buffer, int size) {
	rb->buffer = buffer;
	rb->len = size;
	rb->ptr = 0;
}

// 读特定长度读缓冲区块数据
static void *
rb_read(struct read_block *rb, int sz) {
	if (rb->len < sz) {
		return NULL;
	}

	int ptr = rb->ptr;
	rb->ptr += sz;
	rb->len -= sz;
	return rb->buffer + ptr;
}

// 写入nil数据
static inline void
wb_nil(struct write_block *wb) {
	uint8_t n = TYPE_NIL;
	wb_push(wb, &n, 1);
}

// 写入boolean数据
static inline void
wb_boolean(struct write_block *wb, int boolean) {
	// 如果真，和1合并类型并写入。否则，和0合并类型并写入。
	uint8_t n = COMBINE_TYPE(TYPE_BOOLEAN , boolean ? 1 : 0);
	wb_push(wb, &n, 1);
}

// 写入integer整数
static inline void
wb_integer(struct write_block *wb, lua_Integer v) {
	int type = TYPE_NUMBER;
	if (v == 0) {
		// 写入0，只需要写入类型头
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_ZERO);
		wb_push(wb, &n, 1);
	} else if (v != (int32_t)v) {
		// 写入64位整数
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_QWORD);
		int64_t v64 = v;
		wb_push(wb, &n, 1);
		wb_push(wb, &v64, sizeof(v64));
	} else if (v < 0) {
		// 写入32位负整数
		int32_t v32 = (int32_t)v;
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		wb_push(wb, &v32, sizeof(v32));
	} else if (v<0x100) {
		// 写入8位无符号整数
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_BYTE);
		wb_push(wb, &n, 1);
		uint8_t byte = (uint8_t)v;
		wb_push(wb, &byte, sizeof(byte));
	} else if (v<0x10000) {
		// 写入16位无符号整数
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_WORD);
		wb_push(wb, &n, 1);
		uint16_t word = (uint16_t)v;
		wb_push(wb, &word, sizeof(word));
	} else {
		// 写入32位正整数
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		uint32_t v32 = (uint32_t)v;
		wb_push(wb, &v32, sizeof(v32));
	}
}

// 写入双精度浮点数
static inline void
wb_real(struct write_block *wb, double v) {
	uint8_t n = COMBINE_TYPE(TYPE_NUMBER , TYPE_NUMBER_REAL);
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

// 写入用户数据指针地址
static inline void
wb_pointer(struct write_block *wb, void *v) {
	uint8_t n = TYPE_USERDATA;
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

// 写入字符串
static inline void
wb_string(struct write_block *wb, const char *str, int len) {
	/*
		字符串也是复合类型
		如果是短字符串TYPE_SHORT_STRING，和长度len合并类型并写入
		如果是长字符串TYPE_LONG_STRING，再根据长度len判断。如果长度少于64K，和2合并类型并写入。否则，和4合并类型并写入。
		如果是长字符串TYPE_LONG_STRING，还要把长度len在类型之后写入缓冲区
	*/
	if (len < MAX_COOKIE) {
		// 如果长度小于32
		uint8_t n = COMBINE_TYPE(TYPE_SHORT_STRING, len);
		wb_push(wb, &n, 1);
		if (len > 0) {
			wb_push(wb, str, len);
		}
	} else {
		uint8_t n;
		if (len < 0x10000) {
			// 如果长度少于64K
			n = COMBINE_TYPE(TYPE_LONG_STRING, 2);
			wb_push(wb, &n, 1);
			uint16_t x = (uint16_t) len;
			wb_push(wb, &x, 2);
		} else {
			// 如果长度超过64K
			n = COMBINE_TYPE(TYPE_LONG_STRING, 4);
			wb_push(wb, &n, 1);
			uint32_t x = (uint32_t) len;
			wb_push(wb, &x, 4);
		}
		wb_push(wb, str, len);
	}
}

static void pack_one(lua_State *L, struct write_block *b, int index, int depth);

// 写入数组table
static int
wb_table_array(lua_State *L, struct write_block * wb, int index, int depth) {
	// 获取数组长度
	int array_size = lua_rawlen(L,index);
	if (array_size >= MAX_COOKIE-1) {
		// 如果数组长度超过31
		// 把31合并类型并写入，再写入数组长度
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, MAX_COOKIE-1);
		wb_push(wb, &n, 1);
		wb_integer(wb, array_size);
	} else {
		// 如果数组长度小于31
		// 把数组长度合并类型并写入
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, array_size);
		wb_push(wb, &n, 1);
	}

	int i;
	// 遍历数组元素并写入
	for (i=1;i<=array_size;i++) {
		// 把数组元素压栈
		lua_rawgeti(L,index,i);
		// 递归读取
		pack_one(L, wb, -1, depth);
		lua_pop(L,1);
	}

	return array_size;
}

// 写入哈希table
static void
wb_table_hash(lua_State *L, struct write_block * wb, int index, int depth, int array_size) {
	// 先把nil压栈，作为第一个键
	lua_pushnil(L);
	while (lua_next(L, index) != 0) {
		// 从栈顶弹出一个键， 然后把索引指定的表中的一个键值对压栈 （弹出的键之后的 “下一” 对）
		// 先压键后压值，-2是键，-1是值
		if (lua_type(L,-2) == LUA_TNUMBER) {
			if (lua_isinteger(L, -2)) {
				// 如果是数字键
				lua_Integer x = lua_tointeger(L,-2);
				if (x>0 && x<=array_size) {
					// 如果小于数组长度，已经在wb_table_array写入过，所以弹出栈不再做处理
					lua_pop(L,1);
					continue;
				}
			}
		}
		// 分别对键值进行递归写入
		pack_one(L,wb,-2,depth);
		pack_one(L,wb,-1,depth);
		// 把值弹出栈，保留键作为下一次lua_next参数
		lua_pop(L, 1);
	}
	// 最后再写入nil，作为结束标识位
	wb_nil(wb);
}

// 写入元table
static void
wb_table_metapairs(lua_State *L, struct write_block *wb, int index, int depth) {
	// 把0合并类型并写入
	uint8_t n = COMBINE_TYPE(TYPE_TABLE, 0);
	wb_push(wb, &n, 1);
	// 把表复制一份
	lua_pushvalue(L, index);
	/*
		function meta.__pairs(t)
		  return next, t, nil
		end

		function meta.__pairs(t)
		  return function(t, k)
		    local v
		    repeat
		      k, v = next(t, k)
		    until k == nil or theseok(t, k, v)
		    return k, v
		  end, t, nil
		end
	
		执行__pairs元方法，即pairs(t)
		返回3个参数，next函数，表t，以及nil
		for k,v in pairs(t) do body end 就可以迭代表t中的所有键值对

	*/
	lua_call(L, 1, 3);
	/*
		执行完lua_call(L, 1, 3)后，栈上的元素
		-1 nil
		-2 表t
		-3 next函数
	*/
	for(;;) {
		lua_pushvalue(L, -2);
		lua_pushvalue(L, -2);
		/*
			执行完两遍lua_pushvalue(L, -2)后，栈上的元素
			-1 k(第一次循环为nil)
			-2 表t
			-3 k(第一次循环为nil)
			-4 表t	
			-5 next函数
		*/
		lua_copy(L, -5, -3);
		/*
			执行完lua_copy(L, -5, -3)后，栈上的元素
			-1 k(第一次循环为nil)
			-2 表t
			-3 next函数
			-4 表t	
			-5 next函数
		*/
		// 执行next(table[,index])函数，返回index键下一个键值对
		// index为nil，返回table的初始键值对
		lua_call(L, 2, 2);		
		/*
			执行完lua_call(L, 2, 2)后，栈上的元素
			-1 v
			-2 k 
			-3 表t	
			-4 next函数
		*/
		int type = lua_type(L, -2);
		if (type == LUA_TNIL) {
			// 如果返回的键为空，清空栈顶
			lua_pop(L, 4);
			break;
		}
		// 递归写入键
		pack_one(L, wb, -2, depth);
		// 递归写入值
		pack_one(L, wb, -1, depth);
		// 把v弹出栈，保留k作为下一次next参数
		lua_pop(L, 1);	
		/*
			执行完lua_pop(L, 1)后，栈上的元素
			-1 k 
			-2 表t	
			-3 next函数
		*/
	}
	// 最后再写入nil，作为结束标识位
	wb_nil(wb);
}

// 写入table
static void
wb_table(lua_State *L, struct write_block *wb, int index, int depth) {
	luaL_checkstack(L, LUA_MINSTACK, NULL);
	if (index < 0) {
		// 负索引转化为正索引
		index = lua_gettop(L) + index + 1;
	}
	if (luaL_getmetafield(L, index, "__pairs") != LUA_TNIL) {
		// 如果table有__pairs元方法，把__pairs元方法压栈
		wb_table_metapairs(L, wb, index, depth);
	} else {
		// 否则先写数组table，后写哈希table
		int array_size = wb_table_array(L, wb, index, depth);
		wb_table_hash(L, wb, index, depth, array_size);
	}
}

// pack主函数
static void
pack_one(lua_State *L, struct write_block *b, int index, int depth) {
	if (depth > MAX_DEPTH) {
		wb_free(b);
		luaL_error(L, "serialize can't pack too depth table");
	}
	int type = lua_type(L,index);
	switch(type) {
	case LUA_TNIL:
		wb_nil(b);
		break;
	case LUA_TNUMBER: {
		if (lua_isinteger(L, index)) {
			lua_Integer x = lua_tointeger(L,index);
			wb_integer(b, x);
		} else {
			lua_Number n = lua_tonumber(L,index);
			wb_real(b,n);
		}
		break;
	}
	case LUA_TBOOLEAN: 
		wb_boolean(b, lua_toboolean(L,index));
		break;
	case LUA_TSTRING: {
		size_t sz = 0;
		const char *str = lua_tolstring(L,index,&sz);
		wb_string(b, str, (int)sz);
		break;
	}
	case LUA_TLIGHTUSERDATA:
		wb_pointer(b, lua_touserdata(L,index));
		break;
	case LUA_TTABLE: {
		if (index < 0) {
			index = lua_gettop(L) + index + 1;
		}
		wb_table(L, b, index, depth+1);
		break;
	}
	default:
		wb_free(b);
		luaL_error(L, "Unsupport type %s to serialize", lua_typename(L, type));
	}
}

static void
pack_from(lua_State *L, struct write_block *b, int from) {
	int n = lua_gettop(L) - from;
	int i;
	// 遍历lua栈，正索引从低到高，依次写入
	for (i=1;i<=n;i++) {
		pack_one(L, b , from + i, 0);
	}
}

static inline void
invalid_stream_line(lua_State *L, struct read_block *rb, int line) {
	int len = rb->len;
	luaL_error(L, "Invalid serialize stream %d (line:%d)", len, line);
}

#define invalid_stream(L,rb) invalid_stream_line(L,rb,__LINE__)

static lua_Integer
get_integer(lua_State *L, struct read_block *rb, int cookie) {
	switch (cookie) {
	case TYPE_NUMBER_ZERO:
		return 0;
	case TYPE_NUMBER_BYTE: {
		uint8_t n;
		uint8_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		n = *pn;
		return n;
	}
	case TYPE_NUMBER_WORD: {
		uint16_t n;
		uint16_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_DWORD: {
		int32_t n;
		int32_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_QWORD: {
		int64_t n;
		int64_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	default:
		invalid_stream(L,rb);
		return 0;
	}
}

static double
get_real(lua_State *L, struct read_block *rb) {
	double n;
	double * pn = rb_read(rb,sizeof(n));
	if (pn == NULL)
		invalid_stream(L,rb);
	memcpy(&n, pn, sizeof(n));
	return n;
}

static void *
get_pointer(lua_State *L, struct read_block *rb) {
	void * userdata = 0;
	void ** v = (void **)rb_read(rb,sizeof(userdata));
	if (v == NULL) {
		invalid_stream(L,rb);
	}
	memcpy(&userdata, v, sizeof(userdata));
	return userdata;
}

static void
get_buffer(lua_State *L, struct read_block *rb, int len) {
	char * p = rb_read(rb,len);
	if (p == NULL) {
		invalid_stream(L,rb);
	}
	lua_pushlstring(L,p,len);
}

static void unpack_one(lua_State *L, struct read_block *rb);

static void
unpack_table(lua_State *L, struct read_block *rb, int array_size) {
	if (array_size == MAX_COOKIE-1) {
		// 真实数组长度，是一个数字类型
		uint8_t type;
		uint8_t *t = rb_read(rb, sizeof(type));
		if (t==NULL) {
			invalid_stream(L,rb);
		}
		type = *t;
		int cookie = type >> 3;
		// 非浮点数的数字型
		if ((type & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL) {
			invalid_stream(L,rb);
		}
		// 获取真实数组长度
		array_size = get_integer(L,rb,cookie);
	}
	luaL_checkstack(L,LUA_MINSTACK,NULL);
	// 创建一张新的lua表
	lua_createtable(L,array_size,0);
	int i;
	// 遍历数组元素
	for (i=1;i<=array_size;i++) {
		unpack_one(L,rb);
		lua_rawseti(L,-2,i);
	}
	// 遍历键值对
	for (;;) {
		// 键
		unpack_one(L,rb);
		if (lua_isnil(L,-1)) {
			// 如果键为空，遍历结束
			lua_pop(L,1);
			return;
		}
		// 值
		unpack_one(L,rb);
		lua_rawset(L,-3);
	}
}

static void
push_value(lua_State *L, struct read_block *rb, int type, int cookie) {
	switch(type) {
	case TYPE_NIL:
		lua_pushnil(L);
		break;
	case TYPE_BOOLEAN:
		lua_pushboolean(L,cookie);
		break;
	case TYPE_NUMBER:
		if (cookie == TYPE_NUMBER_REAL) {
			lua_pushnumber(L,get_real(L,rb));
		} else {
			lua_pushinteger(L, get_integer(L, rb, cookie));
		}
		break;
	case TYPE_USERDATA:
		lua_pushlightuserdata(L,get_pointer(L,rb));
		break;
	case TYPE_SHORT_STRING:
		get_buffer(L,rb,cookie);
		break;
	case TYPE_LONG_STRING: {
		if (cookie == 2) {
			uint16_t *plen = rb_read(rb, 2);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint16_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		} else {
			if (cookie != 4) {
				invalid_stream(L,rb);
			}
			uint32_t *plen = rb_read(rb, 4);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint32_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		}
		break;
	}
	case TYPE_TABLE: {
		unpack_table(L,rb,cookie);
		break;
	}
	default: {
		invalid_stream(L,rb);
		break;
	}
	}
}

static void
unpack_one(lua_State *L, struct read_block *rb) {
	uint8_t type;
	uint8_t *t = rb_read(rb, sizeof(type));
	if (t==NULL) {
		invalid_stream(L, rb);
	}
	type = *t;
	push_value(L, rb, type & 0x7, type>>3);
}

static void
seri(lua_State *L, struct block *b, int len) {
	// 创建新的缓冲区
	uint8_t * buffer = skynet_malloc(len);
	uint8_t * ptr = buffer;
	int sz = len;
	// 遍历写缓冲区块，复制数据到新缓冲区
	while(len>0) {
		if (len >= BLOCK_SIZE) {
			memcpy(ptr, b->buffer, BLOCK_SIZE);
			ptr += BLOCK_SIZE;
			len -= BLOCK_SIZE;
			b = b->next;
		} else {
			memcpy(ptr, b->buffer, len);
			break;
		}
	}

	// 把新的缓冲区的指针和大小压栈作为lua返回值
	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, sz);
}

// c.unpack(str)/c.unpack(msg, sz)
// skynet.unpack
int
luaseri_unpack(lua_State *L) {
	if (lua_isnoneornil(L,1)) {
		return 0;
	}
	void * buffer;
	int len;
	if (lua_type(L,1) == LUA_TSTRING) {
		// string
		size_t sz;
		buffer = (void *)lua_tolstring(L,1,&sz);
		len = (int)sz;
	} else {
		// msg, sz
		buffer = lua_touserdata(L,1);
		len = luaL_checkinteger(L,2);
	}
	if (len == 0) {
		return 0;
	}
	if (buffer == NULL) {
		return luaL_error(L, "deserialize null pointer");
	}

	// 让缓冲区数据在栈顶
	lua_settop(L,1);
	// 读缓冲区初始化
	struct read_block rb;
	rball_init(&rb, buffer, len);

	int i;
	for (i=0;;i++) {
		// ?
		if (i%8==7) {
			// LUA_MINSTACK 一般定义为20
			luaL_checkstack(L,LUA_MINSTACK,NULL);
		}
		uint8_t type = 0;
		uint8_t *t = rb_read(&rb, sizeof(type));
		if (t==NULL)
			break;
		type = *t;
		/*
			type & 0x7 只保留低三位，获得一级类型 type
			type >> 3 只保留高五位，获得二级类型 cookie
		*/
		push_value(L, &rb, type & 0x7, type>>3);
	}

	// Need not free buffer
	// 为什么不需要释放缓冲区?
	// 因为由框架在回调完用户注册的服务callback函数后释放，无需由用户代码去手动释放内存
	/*		
		if (!ctx->cb(ctx, ctx->cb_ud, type, msg->session, msg->source, msg->data, sz)) {
			skynet_free(msg->data);
		} 
	*/
	
	// 缓冲区buffer在栈底，所以返回参数数量需要-1
	return lua_gettop(L) - 1;
}

// c.pack
// skynet.pack
int
luaseri_pack(lua_State *L) {
	// 声明第一个块变量，编译器为数据缓冲分配栈内存
	struct block temp;
	temp.next = NULL;
	struct write_block wb;
	// 初始化写缓冲区链表
	wb_init(&wb, &temp);
	// 把lua栈中的元素依次打包写入
	pack_from(L,&wb,0);
	assert(wb.head == &temp);
	// 把写缓冲区链表数据复制到新的缓冲区
	seri(L, &temp, wb.len);
	// 释放写缓冲区链表
	wb_free(&wb);

	return 2;
}
