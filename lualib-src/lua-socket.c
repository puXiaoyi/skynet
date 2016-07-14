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

// �������ڵ�
struct buffer_node {
	char * msg;					// ��Ϣָ��
	int sz;						// ��Ϣ��С
	struct buffer_node *next;	// ��һ���ڵ�ָ��
};

// ����������
struct socket_buffer {
	int size;					// ���нڵ����ݵ��ܳ���
	int offset;					// �Ѷ����ݳ���ƫ��
	struct buffer_node *head;	// ͷָ��
	struct buffer_node *tail;	// βָ��
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
// �ͷŻ�������
static int
lfreepool(lua_State *L) {
	// ��ȡ��һ����������������
	struct buffer_node * pool = lua_touserdata(L, 1);
	// lua_rawlen ��ȡ���û����ݷ�����ڴ��Ĵ�С
	// sizeof(*pool) Ϊ����buffer_node�ṹռ���ڴ���С
	// sz Ϊ����صĳ���
	int sz = lua_rawlen(L,1) / sizeof(*pool);
	int i;
	// ����
	for (i=0;i<sz;i++) {
		struct buffer_node *node = &pool[i];
		if (node->msg) {
			skynet_free(node->msg);
			node->msg = NULL;
		}
	}
	return 0;
}

// �½���������
// ����ʽ�����
static int
lnewpool(lua_State *L, int sz) {
	// �½�һ����ȫ�û����ݣ�ѹջ������������
	// ����ָ����С���ڴ�飬ջ�ϱ����ֻ���ڴ��ַ
	struct buffer_node * pool = lua_newuserdata(L, sizeof(struct buffer_node) * sz);
	int i;
	for (i=0;i<sz;i++) {
		pool[i].msg = NULL;
		pool[i].sz = 0;
		pool[i].next = &pool[i+1];
	}
	pool[sz-1].next = NULL;
	// buffer_pool Ԫ��ѹջ
	if (luaL_newmetatable(L, "buffer_pool")) {
		// ���Ԫ���в�����buffer_pool������ΪԪ����һ��buffer_pool�������±�
		// Ϊ�±�����__gc����Ϊlfreepool
		lua_pushcfunction(L, lfreepool);
		lua_setfield(L, -2, "__gc");
	}
	// ��buffer_pool����ջ����������Ϊpool��Ԫ��
	lua_setmetatable(L, -2);
	return 1;
}

// �½�����������
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
	// ��ȡ��һ������ֵ������������
	struct socket_buffer *sb = lua_touserdata(L,1);
	if (sb == NULL) {
		return luaL_error(L, "need buffer object at param 1");
	}
	// ��ȡ����������ֵ����Ϣ����
	char * msg = lua_touserdata(L,3);
	if (msg == NULL) {
		return luaL_error(L, "need message block at param 3");
	}
	int pool_index = 2;
	// ���ڶ��������Ƿ�Ϊtable����
	/*
		local buffer_pool = {}
		buffer_pool����lua��(socket.lua)�д�������ʼ���ձ�
		lua����pop,push,readall,readline,clear����Ϊ�ڶ�����������
		��lua�󶨵�c�����У���ջ��ȡ���ڶ���Ԫ�أ����ɵõ�buffer_pool

		buffer_pool[1]����ŵ���һ��lightuserdata�����������ص�һ���ڵ�ĵ�ַ����lua_pushlightuserdata(L, free_node->next);	
		buffer_pool[2] ...����ŵ���userdata�����̶���С�����������ڴ��ַ����lua_rawseti(L, pool_index, tsz+1);
	*/
	luaL_checktype(L,pool_index,LUA_TTABLE);
	// ��ȡ���ĸ�����ֵ����Ϣ����
	int sz = luaL_checkinteger(L,4);
	// �ѻ����lua��ĵ�һ��Ԫ��ѹջ
	lua_rawgeti(L,pool_index,1);
	// ��ȡ�����lua��ĵ�һ��Ԫ��
	// ׼ȷ��˵�� ��ȡ��һ��Ԫ�ش�ŵ�ָ�� ��ָ���һ����ȫ�û����� ��ʵ����һ���������ڵ�
	struct buffer_node * free_node = lua_touserdata(L,-1);	// sb poolt msg size free_node
	// �ѻ����lua��ĵ�һ��Ԫ�ص���ջ
	lua_pop(L,1);
	if (free_node == NULL) {
		// �����ȡ���Ļ������ڵ�Ϊ�գ�˵��buffer_pool[n]���buffer_node�Ѿ��ù�

		// ��ȡbuffer_pool��ĳ���
		// ��ʼΪ0��һֱ������ֱ��vm����
		/*
			tsz buffer_pool��ĳ���
			index buffer_pool�������
			size buffer_pool[index]���buffer_node������
		
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
			// ��ʼΪ0��Ԥ��buffer_pool[1]���lightuserdata
			tsz++;
		int size = 8;
		if (tsz <= LARGE_PAGE_NODE-3) {
			size <<= tsz;
		} else {
			// ������4096����2��12�η�
			size <<= LARGE_PAGE_NODE-3;
		}
		// �½���������
		// struct buffer_node * pool = lua_newuserdata(L, sizeof(struct buffer_node) * sz);
		lnewpool(L, size);	
		// ��ȡ�½��������ص�һ���������ڵ�
		free_node = lua_touserdata(L,-1);
		// ���½��Ļ������طŵ���������lua����ض�������
		lua_rawseti(L, pool_index, tsz+1);
	}
	// �ѻ����lua��ĵ�һ��Ԫ�ص���һ��Ԫ��ѹջ
	lua_pushlightuserdata(L, free_node->next);	
	// �ѻ����lua��ĵ�һ��Ԫ�ص���һ��Ԫ������Ϊlua��ĵ�һ��Ԫ�أ�������ջ
	lua_rawseti(L, pool_index, 1);	// sb poolt msg size

	// ���ϲ����ӻ������ȡ����һ���������ڵ�

	// �������ڵ�װ������
	free_node->msg = msg;
	free_node->sz = sz;
	free_node->next = NULL;

	// �������ڵ��������
	if (sb->head == NULL) {
		assert(sb->tail == NULL);
		sb->head = sb->tail = free_node;
	} else {
		sb->tail->next = free_node;
		sb->tail = free_node;
	}
	sb->size += sz;

	// ���������������ܳ���ѹջ����Ϊlua����ֵ
	lua_pushinteger(L, sb->size);

	return 1;
}

// �黹ͷָ�뻺�����ڵ�������
// pool ����������ջ�ϵ�����
static void
return_free_node(lua_State *L, int pool, struct socket_buffer *sb) {
 	// �޸�headָ��Ϊ��һ���������ڵ�
	struct buffer_node *free_node = sb->head;
	sb->offset = 0;
	sb->head = free_node->next;
	if (sb->head == NULL) {
		// ���û����һ���ڵ㣬head��tail��������ֵΪNULL
		sb->tail = NULL;
	}
	// �ѻ����lua��ĵ�һ��Ԫ��ѹջ
	lua_rawgeti(L,pool,1);
	// �ѻ����lua��ĵ�һ��Ԫ������ΪҪ�黹�Ļ������ڵ����һ��Ԫ��
	free_node->next = lua_touserdata(L,-1);
	// �ѻ����lua��ĵ�һ��Ԫ�ص���ջ
	lua_pop(L,1);
	// ����Ҫ�黹�Ļ������ڵ�����Ժ��ڴ�
	skynet_free(free_node->msg);
	free_node->msg = NULL;

	free_node->sz = 0;
	// ��Ҫ�黹�Ļ������ڵ�ѹջ
	lua_pushlightuserdata(L, free_node);
	// ��Ҫ�黹�Ļ������ڵ�����Ϊlua��ĵ�һ��Ԫ�أ�������ջ
	lua_rawseti(L, pool, 1);
}

// sz ��Ҫ�������ݳ���
// skip ��Ҫ���������ݳ��� ����\n�ĳ���
static void
pop_lstring(lua_State *L, struct socket_buffer *sb, int sz, int skip) {
	struct buffer_node * current = sb->head;
	if (sz < current->sz - sb->offset) {
		// �����Ҫ���ĳ���С��ʣ��ɶ��ĳ���
		// �ѻ���������ѹ��ջ����Ϊlua����ֵ
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		// ƫ��������
		sb->offset+=sz;
		return;
	}
	if (sz == current->sz - sb->offset) {
		// �����Ҫ���ĳ��ȵ���ʣ��ɶ��ĳ���
		// �ѻ���������ѹ��ջ����Ϊlua����ֵ
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		// ��ǰ�������ڵ��Ѷ��꣬�黹�������ڵ㵽�����
		return_free_node(L,2,sb);
		return;
	}

	// �ַ������������c����ֶι���һ��Lua�ַ���

	// ����һ���ַ�������
	luaL_Buffer b;
	// ��ʼ���ַ�������
	luaL_buffinit(L, &b);

	// ѭ����ȡ���������������
	for (;;) {
		// ��ǰ�������ڵ�ɶ����ݳ���
		int bytes = current->sz - sb->offset;
		if (bytes >= sz) {
			// �����Ҫ���ĳ���С��ʣ��ɶ��ĳ���
			// �տ�ʼ����ѭ��������������߼����׽ڵ��������Ѵ����˺�����Ľڵ��Կ��ܳ��ִ������
			if (sz > skip) {
				// �ѻ��������ݷ����ַ�������
				luaL_addlstring(&b, current->msg + sb->offset, sz - skip);
			} 
			// ƫ��������
			sb->offset += sz;
			if (bytes == sz) {
				// �������ȫ�����꣬�黹�������ڵ㵽�����
				return_free_node(L,2,sb);
			}
			// ��Ҫ�����Ѷ��꣬����ѭ�������ٶ�
			break;
		}
		// ʵ��Ҫ�������ݳ��ȣ���sep�ָ���֮��
		int real_sz = sz - skip;
		if (real_sz > 0) {
			// �ѻ��������ݷ����ַ�������
			luaL_addlstring(&b, current->msg + sb->offset, (real_sz < bytes) ? real_sz : bytes);
		}
		// ��ǰ�������ڵ��Ѷ��꣬�黹�������ڵ㵽�����
		return_free_node(L,2,sb);
		sz-=bytes;
		if (sz==0)
			// ���ȫ�����꣬����ѭ�������ٶ�
			break;
		// ����һ���������ڵ�
		current = sb->head;
		assert(current);
	}
	// �������ַ��������ʹ�ã������յ��ַ�������ջ��
	luaL_pushresult(&b);
}

// driver.header
// socket.header socket.lua
// local sz = socket.header(sock:read(2)) clusterd.lua
static int
lheader(lua_State *L) {
	size_t len;
	const uint8_t * s = (const uint8_t *)luaL_checklstring(L, 1, &len);
	// 1-4���ֽ�
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
		// ��������ݿɶ�������nil
		lua_pushnil(L);
	} else {
		pop_lstring(L,sb,sz,0);
		sb->size -= sz;
	}
	// �ѻ���������ʣ�����ݳ���ѹջ��Ϊlua�ĵڶ�������ֵ
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
		// ���ι黹���л������ڵ�
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
	// ��������������
	while(sb->head) {
		struct buffer_node *current = sb->head;
		// �ѻ������ڵ���������ݶ������ַ�������
		luaL_addlstring(&b, current->msg + sb->offset, current->sz - sb->offset);
		return_free_node(L,2,sb);
	}
	// �������ַ��������ʹ�ã������յ��ַ�������ջ������Ϊlua����ֵ
	luaL_pushresult(&b);
	sb->size = 0;
	return 1;
}

// driver.drop(data, size)
static int
ldrop(lua_State *L) {
	void * msg = lua_touserdata(L,1);
	luaL_checkinteger(L,2);
	// �����ڴ�ռ�
	skynet_free(msg);
	return 0;
}

// ��⻺�����б����Ƿ���sep�ָ���
static bool
check_sep(struct buffer_node * node, int from, const char *sep, int seplen) {
	for (;;) {
		// �ɶ����ݳ���
		int sz = node->sz - from;
		if (sz >= seplen) {
			// �����Ƿ���sep��ͷ
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
// buffer_pool ����أ��������nil �����Ƿ���ڷָ������������table�����ض���Ļ���������
// sep �ָ��� ����\n
static int
lreadline(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	// only check
	// ����ڶ���������lua table��checkΪfalse������checkΪtrue
	bool check = !lua_istable(L, 2);
	size_t seplen = 0;
	// ��ȡ����������ֵ���ָ����ַ�����ͬʱ��ȡ�䳤��
	const char *sep = luaL_checklstring(L,3,&seplen);
	int i;
	struct buffer_node *current = sb->head;
	if (current == NULL)
		return 0;
	int from = sb->offset;
	// ��ǰ�������ڵ�ɶ����ݳ���
	int bytes = current->sz - from;
	for (i=0;i<=sb->size - (int)seplen;i++) {
		if (check_sep(current, from, sep, seplen)) {
			// ����зָ���
			if (check) {
				// ���û�д���buffer_pool�������򷵻�true
				/*
					���� socket.lua
					if driver.readline(s.buffer, nil, rr) then
						s.read_required = nil
						wakeup(s)
					end
				*/
				lua_pushboolean(L,true);
			} else {
				// ���뻺�������ݲ�ѹջ��Ϊlua����ֵ
				pop_lstring(L, sb, i+seplen, seplen);
				sb->size -= i+seplen;
			}
			return 1;
		}
		// ���û�зָ������ƶ�һ���ַ���������
		++from;
		--bytes;
		if (bytes == 0) {
			// ����һ���������ڵ㣬����һ���������ڵ�
			current = current->next;
			from = 0;
			if (current == NULL)
				// ��һ��Ϊ�գ�����ѭ��
				break;
			bytes = current->sz;
		}
	}
	// ���û�зָ�����û�з��ز���
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

	// ����Ϣ������ѹջ��Ϊlua����ֵ
	lua_pushinteger(L, message->type);
	lua_pushinteger(L, message->id);
	lua_pushinteger(L, message->ud);
	if (message->buffer == NULL) {
		// padding������ 
		// SOCKET_OPEN SOCKET_ACCEPT SOCKET_ERROR û�а�data����д�뻺���������Ƿ���֮����ڴ�ռ�
		// ��padding������ѹջ��Ϊlua����ֵ
		lua_pushlstring(L, (char *)(message+1),size - sizeof(*message));
	} else {
		// ����Ϣ������ָ��ѹջ��Ϊlua����ֵ
		lua_pushlightuserdata(L, message->buffer);
	}
	if (message->type == SKYNET_SOCKET_TYPE_UDP) {
		// �����Ϣ��udp���ͣ���UDP��ַѹջ��Ϊlua����ֵ
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
		// ���portֵΪ��
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
		// ���portֵ��Ϊ�գ�ֱ��ȡ������ֵ��Ĭ��Ϊ0
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

// ����lua table�ĳ���
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

// ��lua table���ӳ��ַ���
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
	// ����һ���µı������б� l �еĺ���ע���ȥ
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
	// ��ע������skynet_contextѹ��ջ��
	lua_getfield(L, LUA_REGISTRYINDEX, "skynet_context");
	struct skynet_context *ctx = lua_touserdata(L,-1);
	// ��ȡ���жϷǿ�
	if (ctx == NULL) {
		return luaL_error(L, "Init skynet context first");
	}
	// ���б� l2 �еĺ���ע���ȥ��������һ����ֵskynet_context
	// ��������ֵ��ջ�е�����ֻ��l2�еĺ�����Ч
	luaL_setfuncs(L,l2,1);

	return 1;
}
