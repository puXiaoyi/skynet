#include "skynet.h"

#include "skynet_handle.h"
#include "skynet_server.h"
#include "rwlock.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define DEFAULT_SLOT_SIZE 4
#define MAX_SLOT_SIZE 0x40000000

// handle结构
struct handle_name {
	char * name;
	uint32_t handle;
};

// handle仓库
struct handle_storage {
	struct rwlock lock;		//读写锁

	uint32_t harbor;		// 高8位harbor标识
	uint32_t handle_index;		// 低24位handle索引，保持递增
	int slot_size;			// 默认为4
	struct skynet_context ** slot;	// 上下文缓冲区
	
	int name_cap;			// 名字缓冲区容量，初始为2
	int name_count;			// 名字缓冲区内数量，初始为0
	struct handle_name *name;	// 名字缓冲区，初始大小为cap
};

// handle仓库实例，由skynet_handle_init初始化，由skynet_start调用
static struct handle_storage *H = NULL;

// 注册一个上下文服务，返回唯一的handle
uint32_t
skynet_handle_register(struct skynet_context *ctx) {
	struct handle_storage *s = H;

	// 加写锁
	rwlock_wlock(&s->lock);
	
	for (;;) {
		int i;
		// 遍历上下文缓冲区
		for (i=0;i<s->slot_size;i++) {
			// 计算低24位handle值
			uint32_t handle = (i+s->handle_index) & HANDLE_MASK;
			// 取余作为hash索引
			int hash = handle & (s->slot_size-1);
			if (s->slot[hash] == NULL) {
				// 如果索引对应的缓冲区为空，则把当前上下文服务放入其中
				s->slot[hash] = ctx;
				// handle索引递增
				s->handle_index = handle + 1;
	
				// 解写锁
				rwlock_wunlock(&s->lock);

				// 加上高8位harbor头
				handle |= s->harbor;
				return handle;
			}
		}
		// 扩容上下文缓冲区，但翻倍后大小不能超过24位
		assert((s->slot_size*2 - 1) <= HANDLE_MASK);
		// 创建新的上下文缓冲区
		struct skynet_context ** new_slot = skynet_malloc(s->slot_size * 2 * sizeof(struct skynet_context *));
		memset(new_slot, 0, s->slot_size * 2 * sizeof(struct skynet_context *));
		// 复制旧的缓冲区数据到新的缓冲区
		for (i=0;i<s->slot_size;i++) {
			int hash = skynet_context_handle(s->slot[i]) & (s->slot_size * 2 - 1);
			assert(new_slot[hash] == NULL);
			new_slot[hash] = s->slot[i];
		}
		skynet_free(s->slot);
		s->slot = new_slot;
		s->slot_size *= 2;
	}
}

// 回收handle对应的上下文资源
int
skynet_handle_retire(uint32_t handle) {
	int ret = 0;
	struct handle_storage *s = H;

	// 加写锁
	rwlock_wlock(&s->lock);

	// 取余得到hash索引
	uint32_t hash = handle & (s->slot_size-1);
	// 获得hash索引对应的上下文对象
	struct skynet_context * ctx = s->slot[hash];

	if (ctx != NULL && skynet_context_handle(ctx) == handle) {
		// 如果对象合法，从上下文缓冲区移除
		s->slot[hash] = NULL;
		ret = 1;
		int i;
		int j=0, n=s->name_count;
		// 查找并释放匹配的名字缓冲区
		for (i=0; i<n; ++i) {
			if (s->name[i].handle == handle) {
				skynet_free(s->name[i].name);
				continue;
			} else if (i!=j) {
				s->name[j] = s->name[i];
			}
			++j;
		}
		s->name_count = j;
	} else {
		ctx = NULL;
	}

	// 解写锁
	rwlock_wunlock(&s->lock);

	if (ctx) {
		// 释放对应的上下文资源
		// release ctx may call skynet_handle_* , so wunlock first.
		skynet_context_release(ctx);
	}

	return ret;
}

// 释放所有的上下文资源
void 
skynet_handle_retireall() {
	struct handle_storage *s = H;
	for (;;) {
		int n=0;
		int i;
		// 遍历上下文缓冲区
		for (i=0;i<s->slot_size;i++) {
			// 加读锁
			rwlock_rlock(&s->lock);
			struct skynet_context * ctx = s->slot[i];
			uint32_t handle = 0;
			if (ctx)
				handle = skynet_context_handle(ctx);
			// 解读锁
			rwlock_runlock(&s->lock);
			if (handle != 0) {
				if (skynet_handle_retire(handle)) {
					++n;
				}
			}
		}
		if (n==0)
			return;
	}
}

// 通过handle获得对应的上下文对象
struct skynet_context * 
skynet_handle_grab(uint32_t handle) {
	struct handle_storage *s = H;
	struct skynet_context * result = NULL;

	// 加读锁
	rwlock_rlock(&s->lock);

	// 取余得到hash索引 
	uint32_t hash = handle & (s->slot_size-1);
	// 获得hash索引所对应的上下文对象
	struct skynet_context * ctx = s->slot[hash];
	if (ctx && skynet_context_handle(ctx) == handle) {
		result = ctx;
		skynet_context_grab(result);
	}

	// 解读锁
	rwlock_runlock(&s->lock);

	return result;
}

// 通过name找到对应的handle
uint32_t 
skynet_handle_findname(const char * name) {
	struct handle_storage *s = H;

	rwlock_rlock(&s->lock);

	uint32_t handle = 0;

	int begin = 0;
	int end = s->name_count - 1;
	// 二分查找
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			handle = n->handle;
			break;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}

	rwlock_runlock(&s->lock);

	return handle;
}

// 把名字放入缓冲区
static void
_insert_name_before(struct handle_storage *s, char *name, uint32_t handle, int before) {
	if (s->name_count >= s->name_cap) {
		// 缓冲区已满，加倍扩容
		s->name_cap *= 2;
		// 缓冲区容量不能超过31位
		assert(s->name_cap <= MAX_SLOT_SIZE);
		// 创建新的缓冲区
		struct handle_name * n = skynet_malloc(s->name_cap * sizeof(struct handle_name));
		int i;
		// 复制旧的缓冲区数据到新的缓冲区
		for (i=0;i<before;i++) {
			n[i] = s->name[i];
		}
		for (i=before;i<s->name_count;i++) {
			n[i+1] = s->name[i];
		}
		// 释放旧的缓冲区
		skynet_free(s->name);
		// 修改仓库实例中的缓冲区指针
		s->name = n;
	} else {
		int i;
		//  缓冲区未满，插入索引之后的数据后移
		for (i=s->name_count;i>before;i--) {
			s->name[i] = s->name[i-1];
		}
	}
	// 更新插入索引对应缓冲区数据
	s->name[before].name = name;
	s->name[before].handle = handle;
	s->name_count ++;
}

static const char *
_insert_name(struct handle_storage *s, const char * name, uint32_t handle) {
	int begin = 0;
	int end = s->name_count - 1;
	// 二分查找name是否已存在，存在则直接返回NULL
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		// 比较两个字符串的大小
		// 0，则已存在直接返回
		// 同时保证了缓冲区连续有序
		int c = strcmp(n->name, name);
		if (c==0) {
			return NULL;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}
	char * result = skynet_strdup(name);

	// 把复制的名字放入名字缓冲区
	_insert_name_before(s, result, handle, begin);

	return result;
}

// 为handle对应的上下文设置name
// 如果name有重复返回NULL，否则返回复制的name字符串
const char * 
skynet_handle_namehandle(uint32_t handle, const char *name) {
	rwlock_wlock(&H->lock);

	const char * ret = _insert_name(H, name, handle);

	rwlock_wunlock(&H->lock);

	return ret;
}

// 初始化handle仓库实例
void 
skynet_handle_init(int harbor) {
	assert(H==NULL);
	struct handle_storage * s = skynet_malloc(sizeof(*H));
	s->slot_size = DEFAULT_SLOT_SIZE;
	s->slot = skynet_malloc(s->slot_size * sizeof(struct skynet_context *));
	memset(s->slot, 0, s->slot_size * sizeof(struct skynet_context *));

	rwlock_init(&s->lock);
	// reserve 0 for system
	s->harbor = (uint32_t) (harbor & 0xff) << HANDLE_REMOTE_SHIFT;
	s->handle_index = 1;
	s->name_cap = 2;
	s->name_count = 0;
	s->name = skynet_malloc(s->name_cap * sizeof(struct handle_name));

	H = s;

	// Don't need to free H
}

