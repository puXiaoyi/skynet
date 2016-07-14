#include "skynet_malloc.h"

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>

#include <lua.h>
#include <lauxlib.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include "skynet_socket.h"

#define BACKLOG 32
// 2 ** 12 == 4096
#define LARGE_PAGE_NODE 12
#define BUFFER_LIMIT (256 * 1024)

// 缓冲区节点
struct buffer_node {
	char * msg;					// 消息指针
	int sz;						// 消息大小
	struct buffer_node *next;	// 下一个节点指针
};

// 缓冲区链表
struct socket_buffer {
	int size;					// 所有节点数据的总长度
	int offset;					// 已读数据长度偏移
	struct buffer_node *head;	// 头指针
	struct buffer_node *tail;	// 尾指针
};

/*
	socket.lua
	local buffer_pool = {}	-- store all message buffer object
	
	The table pool record all the buffers chunk, 
	and the first index [1] is a lightuserdata : free_node. We can always use this pointer for struct buffer_node .
	The following ([2] ...)  userdatas in table pool is the buffer chunk (for struct buffer_node), 
	we never free them until the VM closed. The size of first chunk ([2]) is 8 struct buffer_node,
	and the second size is 16 ... The largest size of chunk is LARGE_PAGE_NODE (4096)

*/
// 释放缓冲区池
static int
lfreepool(lua_State *L) {
	// 获取第一个参数，缓冲区池
	struct buffer_node * pool = lua_touserdata(L, 1);
	// lua_rawlen 获取该用户数据分配的内存块的大小
	// sizeof(*pool) 为单个buffer_node结构占用内存块大小
	// sz 为缓冲池的长度
	int sz = lua_rawlen(L,1) / sizeof(*pool);
	int i;
	// 遍历
	for (i=0;i<sz;i++) {
		struct buffer_node *node = &pool[i];
		if (node->msg) {
			skynet_free(node->msg);
			node->msg = NULL;
		}
	}
	return 0;
}

// 新建缓冲区池
// 链表式数组表
static int
lnewpool(lua_State *L, int sz) {
	// 新建一块完全用户数据，压栈当作缓冲区池
	// 分配指定大小的内存块，栈上保存的只是内存地址
	struct buffer_node * pool = lua_newuserdata(L, sizeof(struct buffer_node) * sz);
	int i;
	for (i=0;i<sz;i++) {
		pool[i].msg = NULL;
		pool[i].sz = 0;
		pool[i].next = &pool[i+1];
	}
	pool[sz-1].next = NULL;
	// buffer_pool 元表压栈
	if (luaL_newmetatable(L, "buffer_pool")) {
		// 如果元表中不存在buffer_pool键名，为元表创建一张buffer_pool键名的新表
		// 为新表设置__gc方法为lfreepool
		lua_pushcfunction(L, lfreepool);
		lua_setfield(L, -2, "__gc");
	}
	// 把buffer_pool表弹出栈，并将其设为pool的元表
	lua_setmetatable(L, -2);
	return 1;
}

// 新建缓冲区链表
// local buffer = driver.buffer()
static int
lnewbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_newuserdata(L, sizeof(*sb));	
	sb->size = 0;
	sb->offset = 0;
	sb->head = NULL;
	sb->tail = NULL;
	
	return 1;
}

/*
	userdata send_buffer
	table pool
	lightuserdata msg
	int size

	return size

	Comment: The table pool record all the buffers chunk, 
	and the first index [1] is a lightuserdata : free_node. We can always use this pointer for struct buffer_node .
	The following ([2] ...)  userdatas in table pool is the buffer chunk (for struct buffer_node), 
	we never free them until the VM closed. The size of first chunk ([2]) is 8 struct buffer_node,
	and the second size is 16 ... The largest size of chunk is LARGE_PAGE_NODE (4096)

	lpushbbuffer will get a free struct buffer_node from table pool, and then put the msg/size in it.
	lpopbuffer return the struct buffer_node back to table pool (By calling return_free_node).
 */
 // local sz = driver.push(s.buffer, buffer_pool, data, size)
static int
lpushbuffer(lua_State *L) {
	// 获取第一个参数值，缓冲区链表
	struct socket_buffer *sb = lua_touserdata(L,1);
	if (sb == NULL) {
		return luaL_error(L, "need buffer object at param 1");
	}
	// 获取第三个参数值，消息数据
	char * msg = lua_touserdata(L,3);
	if (msg == NULL) {
		return luaL_error(L, "need message block at param 3");
	}
	int pool_index = 2;
	// 检查第二个参数是否为table类型
	/*
		local buffer_pool = {}
		buffer_pool是在lua层(socket.lua)中创建并初始化空表
		lua方法pop,push,readall,readline,clear中作为第二个参数传入
		在lua绑定的c方法中，从栈中取出第二个元素，即可得到buffer_pool

		buffer_pool[1]，存放的是一个lightuserdata，即缓冲区池第一个节点的地址，见lua_pushlightuserdata(L, free_node->next);	
		buffer_pool[2] ...，存放的是userdata，即固定大小的链表数组内存地址，见lua_rawseti(L, pool_index, tsz+1);
	*/
	luaL_checktype(L,pool_index,LUA_TTABLE);
	// 获取第四个参数值，消息长度
	int sz = luaL_checkinteger(L,4);
	// 把缓冲池lua表的第一个元素压栈
	lua_rawgeti(L,pool_index,1);
	// 获取缓冲池lua表的第一个元素
	// 准确地说是 获取第一个元素存放的指针 所指向的一段完全用户数据 其实就是一个缓冲区节点
	struct buffer_node * free_node = lua_touserdata(L,-1);	// sb poolt msg size free_node
	// 把缓冲池lua表的第一个元素弹出栈
	lua_pop(L,1);
	if (free_node == NULL) {
		// 如果获取到的缓冲区节点为空，说明buffer_pool[n]里的buffer_node已经用光

		// 获取buffer_pool表的长度
		// 初始为0，一直递增，直到vm重启
		/*
			tsz buffer_pool表的长度
			index buffer_pool表的索引
			size buffer_pool[index]存放buffer_node的数量
		
			tsz  size          index
			0    8 << 1 = 16   2
			2    8 << 2 = 32   3
			.
			.
			.
			9    8 << 9 = 4096 10
			10   8 << 9 = 4096 11
		*/
		int tsz = lua_rawlen(L,pool_index);
		if (tsz == 0)
			// 初始为0，预留buffer_pool[1]存放lightuserdata
			tsz++;
		int size = 8;
		if (tsz <= LARGE_PAGE_NODE-3) {
			size <<= tsz;
		} else {
			// 不超过4096，即2的12次方
			size <<= LARGE_PAGE_NODE-3;
		}
		// 新建缓冲区池
		// struct buffer_node * pool = lua_newuserdata(L, sizeof(struct buffer_node) * sz);
		lnewpool(L, size);	
		// 获取新建缓冲区池第一个缓冲区节点
		free_node = lua_touserdata(L,-1);
		// 把新建的缓冲区池放到缓冲区池lua表的特定索引中
		lua_rawseti(L, pool_index, tsz+1);
	}
	// 把缓冲池lua表的第一个元素的下一个元素压栈
	lua_pushlightuserdata(L, free_node->next);	
	// 把缓冲池lua表的第一个元素的下一个元素设置为lua表的第一个元素，并弹出栈
	lua_rawseti(L, pool_index, 1);	// sb poolt msg size

	// 以上操作从缓冲池中取出了一个缓冲区节点

	// 缓冲区节点装入数据
	free_node->msg = msg;
	free_node->sz = sz;
	free_node->next = NULL;

	// 缓冲区节点加入链表
	if (sb->head == NULL) {
		assert(sb->tail == NULL);
		sb->head = sb->tail = free_node;
	} else {
		sb->tail->next = free_node;
		sb->tail = free_node;
	}
	sb->size += sz;

	// 缓冲区链表数据总长度压栈并作为lua返回值
	lua_pushinteger(L, sb->size);

	return 1;
}

// 归还头指针缓冲区节点给缓冲池
// pool 缓冲区池在栈上的索引
static void
return_free_node(lua_State *L, int pool, struct socket_buffer *sb) {
 	// 修改head指针为下一个缓冲区节点
	struct buffer_node *free_node = sb->head;
	sb->offset = 0;
	sb->head = free_node->next;
	if (sb->head == NULL) {
		// 如果没有下一个节点，head和tail索引均赋值为NULL
		sb->tail = NULL;
	}
	// 把缓冲池lua表的第一个元素压栈
	lua_rawgeti(L,pool,1);
	// 把缓冲池lua表的第一个元素设置为要归还的缓冲区节点的下一个元素
	free_node->next = lua_touserdata(L,-1);
	// 把缓冲池lua表的第一个元素弹出栈
	lua_pop(L,1);
	// 清理要归还的缓冲区节点的属性和内存
	skynet_free(free_node->msg);
	free_node->msg = NULL;

	free_node->sz = 0;
	// 把要归还的缓冲区节点压栈
	lua_pushlightuserdata(L, free_node);
	// 把要归还的缓冲区节点设置为lua表的第一个元素，并弹出栈
	lua_rawseti(L, pool, 1);
}

// sz 需要读的数据长度
// skip 需要跳过的数据长度 比如\n的长度
static void
pop_lstring(lua_State *L, struct socket_buffer *sb, int sz, int skip) {
	struct buffer_node * current = sb->head;
	if (sz < current->sz - sb->offset) {
		// 如果需要读的长度小于剩余可读的长度
		// 把缓冲区数据压入栈中作为lua返回值
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		// 偏移量增加
		sb->offset+=sz;
		return;
	}
	if (sz == current->sz - sb->offset) {
		// 如果需要读的长度等于剩余可读的长度
		// 把缓冲区数据压入栈中作为lua返回值
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		// 当前缓冲区节点已读完，归还缓冲区节点到缓冲池
		return_free_node(L,2,sb);
		return;
	}

	// 字符串缓存可以让c代码分段构造一个Lua字符串

	// 定义一个字符串缓存
	luaL_Buffer b;
	// 初始化字符串缓存
	luaL_buffinit(L, &b);

	// 循环读取缓冲区链表的数据
	for (;;) {
		// 当前缓冲区节点可读数据长度
		int bytes = current->sz - sb->offset;
		if (bytes >= sz) {
			// 如果需要读的长度小于剩余可读的长度
			// 刚开始进入循环，不会走这段逻辑，首节点此种情况已处理，此后遍历的节点仍可能出现此种情况
			if (sz > skip) {
				// 把缓冲区数据放入字符串缓存
				luaL_addlstring(&b, current->msg + sb->offset, sz - skip);
			} 
			// 偏移量增加
			sb->offset += sz;
			if (bytes == sz) {
				// 如果正好全部读完，归还缓冲区节点到缓冲池
				return_free_node(L,2,sb);
			}
			// 需要读的已读完，跳出循环不必再读
			break;
		}
		// 实际要读的数据长度，除sep分隔符之外
		int real_sz = sz - skip;
		if (real_sz > 0) {
			// 把缓冲区数据放入字符串缓存
			luaL_addlstring(&b, current->msg + sb->offset, (real_sz < bytes) ? real_sz : bytes);
		}
		// 当前缓冲区节点已读完，归还缓冲区节点到缓冲池
		return_free_node(L,2,sb);
		sz-=bytes;
		if (sz==0)
			// 如果全部读完，跳出循环不必再读
			break;
		// 换下一个缓冲区节点
		current = sb->head;
		assert(current);
	}
	// 结束对字符串缓存的使用，将最终的字符串留在栈顶
	luaL_pushresult(&b);
}

// driver.header
// socket.header socket.lua
// local sz = socket.header(sock:read(2)) clusterd.lua
static int
lheader(lua_State *L) {
	size_t len;
	const uint8_t * s = (const uint8_t *)luaL_checklstring(L, 1, &len);
	// 1-4个字节
	if (len > 4 || len < 1) {
		return luaL_error(L, "Invalid read %s", s);
	}
	int i;
	size_t sz = 0;
	for (i=0;i<(int)len;i++) {
		sz <<= 8;
		sz |= s[i];
	}

	lua_pushinteger(L, (lua_Integer)sz);

	return 1;
}

/*
	userdata send_buffer
	table pool
	integer sz 
 */
// local ret = driver.pop(s.buffer, buffer_pool, sz)
// socket.read socket.lua
// return nil / return msg, sz
static int
lpopbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	int sz = luaL_checkinteger(L,3);
	if (sb->size < sz || sz == 0) {
		// 如果无数据可读，返回nil
		lua_pushnil(L);
	} else {
		pop_lstring(L,sb,sz,0);
		sb->size -= sz;
	}
	// 把缓冲区链表剩余数据长度压栈作为lua的第二个返回值
	lua_pushinteger(L, sb->size);

	return 2;
}

/*
	userdata send_buffer
	table pool
 */
// driver.clear(s.buffer, buffer_pool)
static int
lclearbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	while(sb->head) {
		// 依次归还所有缓冲区节点
		return_free_node(L,2,sb);
	}
	sb->size = 0;
	return 0;
}

// local ret = driver.readall(s.buffer, buffer_pool)
// socket.read(id, sz) socket.lua
static int
lreadall(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	luaL_Buffer b;
	luaL_buffinit(L, &b);
	// 遍历缓冲区链表
	while(sb->head) {
		struct buffer_node *current = sb->head;
		// 把缓冲区节点的所有数据都放入字符串缓存
		luaL_addlstring(&b, current->msg + sb->offset, current->sz - sb->offset);
		return_free_node(L,2,sb);
	}
	// 结束对字符串缓存的使用，将最终的字符串留在栈顶，作为lua返回值
	luaL_pushresult(&b);
	sb->size = 0;
	return 1;
}

// driver.drop(data, size)
static int
ldrop(lua_State *L) {
	void * msg = lua_touserdata(L,1);
	luaL_checkinteger(L,2);
	// 清理内存空间
	skynet_free(msg);
	return 0;
}

// 检测缓冲区列表中是否有sep分隔符
static bool
check_sep(struct buffer_node * node, int from, const char *sep, int seplen) {
	for (;;) {
		// 可读数据长度
		int sz = node->sz - from;
		if (sz >= seplen) {
			// 数据是否以sep开头
			return memcmp(node->msg+from,sep,seplen) == 0;
		}
		if (sz > 0) {
			if (memcmp(node->msg + from, sep, sz)) {
				return false;
			}
		}
		node = node->next;
		sep += sz;
		seplen -= sz;
		from = 0;
	}
}

/*
	userdata send_buffer
	table pool , nil for check
	string sep
 */
// local ret = driver.readline(s.buffer, buffer_pool, sep)
// return msg/true/nil
// buffer_pool 缓冲池，如果传入nil 返回是否存在分隔符，如果传入table，返回读入的缓冲区数据
// sep 分隔符 比如\n
static int
lreadline(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	// only check
	// 如果第二个参数是lua table，check为false，否则check为true
	bool check = !lua_istable(L, 2);
	size_t seplen = 0;
	// 获取第三个参数值，分隔符字符串，同时获取其长度
	const char *sep = luaL_checklstring(L,3,&seplen);
	int i;
	struct buffer_node *current = sb->head;
	if (current == NULL)
		return 0;
	int from = sb->offset;
	// 当前缓冲区节点可读数据长度
	int bytes = current->sz - from;
	for (i=0;i<=sb->size - (int)seplen;i++) {
		if (check_sep(current, from, sep, seplen)) {
			// 如果有分隔符
			if (check) {
				// 如果没有传入buffer_pool参数，则返回true
				/*
					比如 socket.lua
					if driver.readline(s.buffer, nil, rr) then
						s.read_required = nil
						wakeup(s)
					end
				*/
				lua_pushboolean(L,true);
			} else {
				// 读入缓冲区数据并压栈作为lua返回值
				pop_lstring(L, sb, i+seplen, seplen);
				sb->size -= i+seplen;
			}
			return 1;
		}
		// 如果没有分隔符，移动一个字符继续检索
		++from;
		--bytes;
		if (bytes == 0) {
			// 读完一个缓冲区节点，换下一个缓冲区节点
			current = current->next;
			from = 0;
			if (current == NULL)
				// 下一个为空，跳出循环
				break;
			bytes = current->sz;
		}
	}
	// 如果没有分隔符，没有返回参数
	return 0;
}

// driver.str2p(str)
// return msg, sz
// str -> msg, sz
static int
lstr2p(lua_State *L) {
	size_t sz = 0;
	const char * str = luaL_checklstring(L,1,&sz);
	void *ptr = skynet_malloc(sz);
	memcpy(ptr, str, sz);
	lua_pushlightuserdata(L, ptr);
	lua_pushinteger(L, (int)sz);
	return 2;
}

// for skynet socket

/*
	lightuserdata msg
	integer size

	return type n1 n2 ptr_or_string
*/
// driver.unpack(msg, sz)
static int
lunpack(lua_State *L) {
	struct skynet_socket_message *message = lua_touserdata(L,1);
	int size = luaL_checkinteger(L,2);

	// 把消息的属性压栈作为lua返回值
	lua_pushinteger(L, message->type);
	lua_pushinteger(L, message->id);
	lua_pushinteger(L, message->ud);
	if (message->buffer == NULL) {
		// padding的数据 
		// SOCKET_OPEN SOCKET_ACCEPT SOCKET_ERROR 没有把data数据写入缓冲区，而是放入之后的内存空间
		// 把padding的数据压栈作为lua返回值
		lua_pushlstring(L, (char *)(message+1),size - sizeof(*message));
	} else {
		// 把消息缓冲区指针压栈作为lua返回值
		lua_pushlightuserdata(L, message->buffer);
	}
	if (message->type == SKYNET_SOCKET_TYPE_UDP) {
		// 如果消息是udp类型，把UDP地址压栈作为lua返回值
		int addrsz = 0;
		const char * addrstring = skynet_socket_udp_address(message, &addrsz);
		if (addrstring) {
			lua_pushlstring(L, addrstring, addrsz);
			return 5;
		}
	}
	return 4;
}

static const char *
address_port(lua_State *L, char *tmp, const char * addr, int port_index, int *port) {
	const char * host;
	if (lua_isnoneornil(L,port_index)) {
		// 如果port值为空
		host = strchr(addr, '[');
		if (host) {
			// is ipv6
			// [addr:port]
			++host;
			const char * sep = strchr(addr,']');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			memcpy(tmp, host, sep-host);
			tmp[sep-host] = '\0';
			host = tmp;
			sep = strchr(sep + 1, ':');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			*port = strtoul(sep+1,NULL,10);
		} else {
			// is ipv4
			// addr:port
			const char * sep = strchr(addr,':');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			memcpy(tmp, addr, sep-addr);
			tmp[sep-addr] = '\0';
			host = tmp;
			*port = strtoul(sep+1,NULL,10);
		}
	} else {
		// 如果port值不为空，直接取出整数值，默认为0
		host = addr;
		*port = luaL_optinteger(L,port_index, 0);
	}
	return host;
}

// local id = driver.connect(addr, port)
// socket.open(addr, port) 
// socket.lua
static int
lconnect(lua_State *L) {
	size_t sz = 0;
	const char * addr = luaL_checklstring(L,1,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = address_port(L, tmp, addr, 2, &port);
	if (port == 0) {
		return luaL_error(L, "Invalid port");
	}
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	// skynet_socket_connect 	skynet_socket
	// socket_server_connect	socket_server
	// send_request 'O'
	// open_socket
	int id = skynet_socket_connect(ctx, host, port);
	lua_pushinteger(L, id);

	return 1;
}

// driver.close(id)
// socket.close_fd(fd) 
// socket.lua
static int
lclose(lua_State *L) {
	int id = luaL_checkinteger(L,1);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	// skynet_socket_close	skynet_socket
	// socket_server_close	socket_server
	// send_request 'K'
	// close_socket
	skynet_socket_close(ctx, id);
	return 0;
}

// driver.shutdown(id)
// socket.shutdown(fd) 
// socket.lua
static int
lshutdown(lua_State *L) {
	int id = luaL_checkinteger(L,1);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	// skynet_socket_shutdown	skynet_socket
	// socket_server_shutdown	socket_server
	// send_request 'K'
	// close_socket 
	// force_close
	skynet_socket_shutdown(ctx, id);
	return 0;
}

// driver.listen(host, port, backlog)
// socket.listen(host, port, backlog)
// socket.lua
static int
llisten(lua_State *L) {
	const char * host = luaL_checkstring(L,1);
	int port = luaL_checkinteger(L,2);
	int backlog = luaL_optinteger(L,3,BACKLOG);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	// skynet_socket_listen	skynet_socket
	// socket_server_listen	socket_server
	// send_request 'L'
	// listen_socket 
	int id = skynet_socket_listen(ctx, host,port,backlog);
	if (id < 0) {
		return luaL_error(L, "Listen error");
	}

	lua_pushinteger(L,id);
	return 1;
}

// 计算lua table的长度
static size_t
count_size(lua_State *L, int index) {
	size_t tlen = 0;
	int i;
	for (i=1;lua_geti(L, index, i) != LUA_TNIL; ++i) {
		size_t len;
		luaL_checklstring(L, -1, &len);
		tlen += len;
		lua_pop(L,1);
	}
	lua_pop(L,1);
	return tlen;
}

// 把lua table连接成字符串
static void
concat_table(lua_State *L, int index, void *buffer, size_t tlen) {
	char *ptr = buffer;
	int i;
	for (i=1;lua_geti(L, index, i) != LUA_TNIL; ++i) {
		size_t len;
		const char * str = lua_tolstring(L, -1, &len);
		if (str == NULL || tlen < len) {
			break;
		}
		memcpy(ptr, str, len);
		ptr += len;
		tlen -= len;
		lua_pop(L,1);
	}
	if (tlen != 0) {
		skynet_free(buffer);
		luaL_error(L, "Invalid strings table");
	}
	lua_pop(L,1);
}

static void *
get_buffer(lua_State *L, int index, int *sz) {
	void *buffer;
	switch(lua_type(L, index)) {
		const char * str;
		size_t len;
	case LUA_TUSERDATA:
	case LUA_TLIGHTUSERDATA:
		buffer = lua_touserdata(L,index);
		*sz = luaL_checkinteger(L,index+1);
		break;
	case LUA_TTABLE:
		// concat the table as a string
		len = count_size(L, index);
		buffer = skynet_malloc(len);
		concat_table(L, index, buffer, len);
		*sz = (int)len;
		break;
	default:
		str =  luaL_checklstring(L, index, &len);
		buffer = skynet_malloc(len);
		memcpy(buffer, str, len);
		*sz = (int)len;
		break;
	}
	return buffer;
}

// driver.send
// socket.write	socket.lua
// socket_write(fd , request) socketchannel.lua
static int
lsend(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	int sz = 0;
	void *buffer = get_buffer(L, 2, &sz);
	// skynet_socket_send	skynet_socket
	// socket_server_send	socket_server
	// send_request 'D'
	// send_socket 
	int err = skynet_socket_send(ctx, id, buffer, sz);
	lua_pushboolean(L, !err);
	return 1;
}

// driver.lsend
// socket.lwrite socket.lua
// socket_lwrite(fd, v) socketchannel.lua
static int
lsendlow(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	int sz = 0;
	void *buffer = get_buffer(L, 2, &sz);
	skynet_socket_send_lowpriority(ctx, id, buffer, sz);
	return 0;
}

// driver.bind
// socket.bind socket.lua
static int
lbind(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int fd = luaL_checkinteger(L, 1);
	// skynet_socket_bind	skynet_socket
	// socket_server_bind	socket_server
	// send_request 'B'
	// bind_socket 
	int id = skynet_socket_bind(ctx,fd);
	lua_pushinteger(L,id);
	return 1;
}

// driver.start
// socket.start socket.lua
static int
lstart(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	// skynet_socket_start	skynet_socket
	// socket_server_start	socket_server
	// send_request 'S'
	// start_socket 
	skynet_socket_start(ctx,id);
	return 0;
}

// driver.nodelay
// socketdriver.nodelay(fd) socketchannel.lua
static int
lnodelay(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	// skynet_socket_nodelay	skynet_socket
	// socket_server_nodelay	socket_server
	// send_request 'T'
	// setopt_socket 
	skynet_socket_nodelay(ctx,id);
	return 0;
}

// driver.udp(host, port)
// socket.udp(callback, host, port) socket.lua
static int
ludp(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	size_t sz = 0;
	const char * addr = lua_tolstring(L,1,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = NULL;
	if (addr) {
		host = address_port(L, tmp, addr, 2, &port);
	}

	// skynet_socket_udp	skynet_socket
	// socket_server_udp	socket_server
	// send_request 'U'
	// add_udp_socket 
	int id = skynet_socket_udp(ctx, host, port);
	if (id < 0) {
		return luaL_error(L, "udp init failed");
	}
	lua_pushinteger(L, id);
	return 1;
}


// driver.udp_connect(id, addr, port)
// socket.udp_connect(id, addr, port, callback) socket.lua
static int
ludp_connect(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	size_t sz = 0;
	const char * addr = luaL_checklstring(L,2,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = NULL;
	if (addr) {
		host = address_port(L, tmp, addr, 3, &port);
	}

	// skynet_socket_udp_connect	skynet_socket
	// socket_server_udp_connect	socket_server
	// send_request 'C'
	// set_udp_address 
	if (skynet_socket_udp_connect(ctx, id, host, port)) {
		return luaL_error(L, "udp connect failed");
	}

	return 0;
}

// driver.udp_send
// socket.sendto(fd, addr, msg) socket.lua
static int
ludp_send(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	const char * address = luaL_checkstring(L, 2);
	int sz = 0;
	void *buffer = get_buffer(L, 3, &sz);

	// skynet_socket_udp_send	skynet_socket
	// socket_server_udp_send	socket_server
	// send_request 'A'
	// send_socket 
	int err = skynet_socket_udp_send(ctx, id, address, buffer, sz);

	lua_pushboolean(L, !err);

	return 1;
}

// driver.udp_address(addr)
// socket.udp_address socket.lua 
// return addr port
static int
ludp_address(lua_State *L) {
	size_t sz = 0;
	const uint8_t * addr = (const uint8_t *)luaL_checklstring(L, 1, &sz);
	uint16_t port = 0;
	memcpy(&port, addr+1, sizeof(uint16_t));
	port = ntohs(port);
	const void * src = addr+3;
	char tmp[256];
	int family;
	if (sz == 1+2+4) {
		family = AF_INET;
	} else {
		if (sz != 1+2+16) {
			return luaL_error(L, "Invalid udp address");
		}
		family = AF_INET6;
	}
	if (inet_ntop(family, src, tmp, sizeof(tmp)) == NULL) {
		return luaL_error(L, "Invalid udp address");
	}
	lua_pushstring(L, tmp);
	lua_pushinteger(L, port);
	return 2;
}

// require "socketdriver"
int
luaopen_socketdriver(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "buffer", lnewbuffer },
		{ "push", lpushbuffer },
		{ "pop", lpopbuffer },
		{ "drop", ldrop },
		{ "readall", lreadall },
		{ "clear", lclearbuffer },
		{ "readline", lreadline },
		{ "str2p", lstr2p },
		{ "header", lheader },

		{ "unpack", lunpack },
		{ NULL, NULL },
	};
	// 创建一张新的表，并把列表 l 中的函数注册进去
	luaL_newlib(L,l);
	luaL_Reg l2[] = {
		{ "connect", lconnect },
		{ "close", lclose },
		{ "shutdown", lshutdown },
		{ "listen", llisten },
		{ "send", lsend },
		{ "lsend", lsendlow },
		{ "bind", lbind },
		{ "start", lstart },
		{ "nodelay", lnodelay },
		{ "udp", ludp },
		{ "udp_connect", ludp_connect },
		{ "udp_send", ludp_send },
		{ "udp_address", ludp_address },
		{ NULL, NULL },
	};
	// 把注册表变量skynet_context压入栈中
	lua_getfield(L, LUA_REGISTRYINDEX, "skynet_context");
	struct skynet_context *ctx = lua_touserdata(L,-1);
	// 获取并判断非空
	if (ctx == NULL) {
		return luaL_error(L, "Init skynet context first");
	}
	// 把列表 l2 中的函数注册进去，并设置一个上值skynet_context
	// 设置完上值从栈中弹出，只对l2中的函数有效
	luaL_setfuncs(L,l2,1);

	return 1;
}
