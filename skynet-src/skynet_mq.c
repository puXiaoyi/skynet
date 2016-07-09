#include "skynet.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#define DEFAULT_QUEUE_SIZE 64
#define MAX_GLOBAL_MQ 0x10000

// 0 means mq is not in global mq.
// 1 means mq is in global mq , or the message is dispatching.

#define MQ_IN_GLOBAL 1
#define MQ_OVERLOAD 1024

//上下文消息队列
struct message_queue {
	struct spinlock lock;		// 回旋锁
	uint32_t handle;		// 所属上下文的handle
	int cap;			// 容量，默认64
	int head;			// 头索引
	int tail;			// 尾索引
	int release;		        // 释放标识，在skynet_mq_mark_release中被赋值	
	int in_global;			// 在全局队列中的标识，默认为1
	int overload;			// 记录超过阈值后的队列长度，默认为0
	int overload_threshold;		// 过载阈值，默认为1024
	struct skynet_message *queue;	// 消息队列缓冲区，初始分配cap长度的内存
	struct message_queue *next;	// 下一个消息队列指针
};

// 全局队列
struct global_queue {
	struct message_queue *head;	// 头指针
	struct message_queue *tail;	// 尾指针
	struct spinlock lock;		// 回旋锁
};

// 全局队列实例, 在skynet_mq_init中创建，在skynet_start被调用
static struct global_queue *Q = NULL;

// 压入全局队列
void 
skynet_globalmq_push(struct message_queue * queue) {
	struct global_queue *q= Q;

	//加锁
	SPIN_LOCK(q)
	assert(queue->next == NULL);
	if(q->tail) {
		// 全局队列不为空
		// 修改当前尾指针对应的消息队列的下一个消息队列指针指向压入的消息队列
		q->tail->next = queue;
		// 修改当前尾指针指向压入的消息队列
		q->tail = queue;
	} else {
		// 全局队列为空
		// 修改当前头指针和尾指针同时指向压入的消息队列
		q->head = q->tail = queue;
	}
	// 解锁
	SPIN_UNLOCK(q)
}

// 弹出全局队列
struct message_queue * 
skynet_globalmq_pop() {
	struct global_queue *q = Q;

	// 加锁
	SPIN_LOCK(q)
	// 弹出头指针指向的消息队列
	struct message_queue *mq = q->head;
	if(mq) {
		// 修改头指针指向下一个消息队列
		q->head = mq->next;
		if(q->head == NULL) {
			assert(mq == q->tail);
			q->tail = NULL;
		}
		mq->next = NULL;
	}
	// 解锁
	SPIN_UNLOCK(q)

	return mq;
}

// 创建消息队列
struct message_queue * 
skynet_mq_create(uint32_t handle) {
	struct message_queue *q = skynet_malloc(sizeof(*q));
	q->handle = handle;
	q->cap = DEFAULT_QUEUE_SIZE;
	q->head = 0;
	q->tail = 0;
	SPIN_INIT(q)
	// When the queue is create (always between service create and service init) ,
	// set in_global flag to avoid push it to global queue .
	// If the service init success, skynet_context_new will call skynet_mq_push to push it to global queue.
	// 之所以初始化为1，保证在服务成功创建并初始化后再把服务的消息队列压入全局队列
	q->in_global = MQ_IN_GLOBAL;
	q->release = 0;
	q->overload = 0;
	q->overload_threshold = MQ_OVERLOAD;
	q->queue = skynet_malloc(sizeof(struct skynet_message) * q->cap);
	q->next = NULL;

	return q;
}

static void 
_release(struct message_queue *q) {
	assert(q->next == NULL);
	SPIN_DESTROY(q)
	skynet_free(q->queue);
	skynet_free(q);
}

// 返回消息队列所属上下文（服务） 的handle值
uint32_t 
skynet_mq_handle(struct message_queue *q) {
	return q->handle;
}

// 返回消息队列中消息数量
int
skynet_mq_length(struct message_queue *q) {
	int head, tail,cap;

	SPIN_LOCK(q)
	head = q->head;
	tail = q->tail;
	cap = q->cap;
	SPIN_UNLOCK(q)
	
	if (head <= tail) {
		return tail - head;
	}
	return tail + cap - head;
}

// 返回超过阈值后的消息数量，如果没有超过阈值返回0
int
skynet_mq_overload(struct message_queue *q) {
	if (q->overload) {
		int overload = q->overload;
		q->overload = 0;
		return overload;
	} 
	return 0;
}

// 弹出队列缓冲区，如果队列为空返回1，否则返回0
int
skynet_mq_pop(struct message_queue *q, struct skynet_message *message) {
	int ret = 1;
	// 加锁
	SPIN_LOCK(q)
	
	if (q->head != q->tail) {
		// 如果消息队列不为空
		// 返回head索引对应缓冲区的消息，同时head递增
		*message = q->queue[q->head++];
		ret = 0;
		int head = q->head;
		int tail = q->tail;
		int cap = q->cap;

		if (head >= cap) {
			//超过容量，索引回绕
			q->head = head = 0;
		}
		
		// 计算缓冲区消息的数量
		int length = tail - head;
		if (length < 0) {
			length += cap;
		}

		// 如果消息的数量超过了过载阈值，过载阈值加倍并记录当前数量
		// 由于cap先于overload_threshold在skynet_mq_push翻倍
		while (length > q->overload_threshold) {
			q->overload = length;
			q->overload_threshold *= 2;
		}
	} else {
		// reset overload_threshold when queue is empty
		// 如果消息队列为空，修改过载阈值为默认值
		q->overload_threshold = MQ_OVERLOAD;
	}

	if (ret) {
		// 如果消息队列为空，修改在全局队列中的标识为0
		q->in_global = 0;
	}
	
	// 解锁
	SPIN_UNLOCK(q)

	return ret;
}

// 扩容队列缓冲区
static void
expand_queue(struct message_queue *q) {
	// cap翻倍，并分配内存创建一个新的消息缓冲区
	struct skynet_message *new_queue = skynet_malloc(sizeof(struct skynet_message) * q->cap * 2);
	int i;
	// 复制旧的缓冲区指针数据到新的缓冲区中
	for (i=0;i<q->cap;i++) {
		new_queue[i] = q->queue[(q->head + i) % q->cap];
	}
	// 重置消息队列变量
	q->head = 0;
	q->tail = q->cap;
	q->cap *= 2;
	
	// 释放老的缓冲区内存
	skynet_free(q->queue);
	// 更新队列缓冲区为新
	q->queue = new_queue;
}

// 压入队列缓冲区
void 
skynet_mq_push(struct message_queue *q, struct skynet_message *message) {
	assert(message);
	// 加锁
	SPIN_LOCK(q)

	// 修改tail索引对应缓冲区存放压入消息的指针
	q->queue[q->tail] = *message;
	if (++ q->tail >= q->cap) {
		// 回绕
		q->tail = 0;
	}

	// 如果缓冲区慢，扩容缓冲区
	if (q->head == q->tail) {
		expand_queue(q);
	}

	if (q->in_global == 0) {
		// 如果之前缓冲区为空从全局队列中弹出，重新压入全局队列并更新标识
		q->in_global = MQ_IN_GLOBAL;
		skynet_globalmq_push(q);
	}
	
	// 解锁
	SPIN_UNLOCK(q)
}

// 初始化全局消息队列
void 
skynet_mq_init() {
	struct global_queue *q = skynet_malloc(sizeof(*q));
	memset(q,0,sizeof(*q));
	SPIN_INIT(q);
	Q=q;
}

// 标识队列可释放
void 
skynet_mq_mark_release(struct message_queue *q) {
	SPIN_LOCK(q)
	assert(q->release == 0);
	q->release = 1;
	if (q->in_global != MQ_IN_GLOBAL) {
		skynet_globalmq_push(q);
	}
	SPIN_UNLOCK(q)
}

static void
_drop_queue(struct message_queue *q, message_drop drop_func, void *ud) {
	struct skynet_message msg;
	while(!skynet_mq_pop(q, &msg)) {
		drop_func(&msg, ud);
	}
	_release(q);
}

void 
skynet_mq_release(struct message_queue *q, message_drop drop_func, void *ud) {
	SPIN_LOCK(q)
	
	if (q->release) {
		SPIN_UNLOCK(q)
		_drop_queue(q, drop_func, ud);
	} else {
		skynet_globalmq_push(q);
		SPIN_UNLOCK(q)
	}
}
