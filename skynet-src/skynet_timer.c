#include "skynet.h"

#include "skynet_timer.h"
#include "skynet_mq.h"
#include "skynet_server.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <time.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(__APPLE__)
#include <sys/time.h>
#endif

typedef void (*timer_execute_func)(void *ud,void *arg);

#define TIME_NEAR_SHIFT 8
#define TIME_NEAR (1 << TIME_NEAR_SHIFT)
#define TIME_LEVEL_SHIFT 6
#define TIME_LEVEL (1 << TIME_LEVEL_SHIFT)
#define TIME_NEAR_MASK (TIME_NEAR-1)
#define TIME_LEVEL_MASK (TIME_LEVEL-1)

/*
	场景元素：
	有1个大厅，4个候场区
	有5种颜色的球，除了红球，还有橙、黄、绿、蓝这4种颜色的球
	有1个面试官
	等待n个面试者

	规则如下：
	1.面试官每秒钟获得1个红球(面试官初始不拥有任意颜色的球)
	2.
		拥有255个红球，再获得1个红球，直接兑换成1个橙球
		拥有255个红球，63个橙球，再获得1个红球，直接兑换成1个黄球
		拥有255个红球，63个橙球，63个黄球，再获得1个红球，直接兑换成1个绿球
		拥有255个红球，63个橙球，63个黄球，63个绿球，再获得1个红球，直接兑换成1个蓝球
		拥有255个红球，63个橙球，63个黄球，63个绿球，63个蓝球，再获得1个红球，所有球都收回
	3.面试者进场，可以选择比面试官多n个红球＃(n小于2<<32)，根据规则2兑换成五种颜色的球，从蓝球开始和面试官比较球的数量
	  	如果蓝球数量不同，面试者进入4号候场区，否则继续判断绿球数量
	  	如果绿球数量不同，面试者进入3号候场区，否则继续判断黄球数量
	  	如果黄球数量不同，面试者进入2号候场区，否则继续判断橙球数量
	  	如果橙球数量不同，面试者进入1号候场区，否则直接进入大厅
	4.面试官的红球数量变为0时，会其他球的数量从候场区选人进入大厅
		如果橙球的数量大于0，从1号候场区选择和自己橙球的数量相同的面试者进入大厅，否则继续判断黄球的数量
		如果黄球的数量大于0，从2号候场区选择和自己黄球的数量相同的面试者进入大厅，否则继续判断绿球的数量
		如果绿球的数量大于0，从3号候场区选择和自己绿球的数量相同的面试者进入大厅，否则继续判断蓝球的数量
		如果蓝球的数量大于0，从4号候场区选择和自己蓝球的数量相同的面试者进入大厅，否则什么都不做
	5.面试官每秒钟在获取红球(1)，兑换红球(2)、候场区选人(4)后，从大厅挑选和自己红球的数量相同的面试者，被选中的面试者获取陈述的权利
*/

// 定时器事件结构
struct timer_event {
	uint32_t handle;
	int session;
};

// 定时器节点结构
struct timer_node {
	struct timer_node *next;	// 下一个节点指针
	uint32_t expire;			// 过期触发时间
};

// 定时器链表
/*
	为什么一个是指针变量，一个是结构变量
	因为head.next才是第一个定时器节点，head只起到头占位作用
	link_clear 返回第一个节点，尾指针指向头占位节点
	link 尾指针指向新节点，同时链表内节点依次串联

*/
struct link_list {
	struct timer_node head;		// 头节点
	struct timer_node *tail;	// 尾指针
};

// 定时器结构
struct timer {
	/*
		near，低8位slot数组，timer_execute中计算time低8位取相应的slot链表执行
		t，高24位分为4个等级，每个等级6位。一级索引为0-3等级，二级索引为对应6位的数值
		具体存放规则，见add_node方法
	*/
	struct link_list near[TIME_NEAR];
	struct link_list t[4][TIME_LEVEL];
	struct spinlock lock;		// 回旋锁
	uint32_t time;				// 递增计数，初始为0，在timer_shift中+1，当time为2<<32时，+1后溢出为0
	uint32_t starttime;			// 启动时的UTC时间秒数	
	uint64_t current;			// 启动后的skynet单位时间数
	uint64_t current_point;		// 当前时刻的skynet单位时间数
};

static struct timer * TI = NULL;

// 清空link_list
static inline struct timer_node *
link_clear(struct link_list *list) {
	struct timer_node * ret = list->head.next;
	list->head.next = 0;
	list->tail = &(list->head);

	return ret;
}

// 链接一个定时器节点
static inline void
link(struct link_list *list,struct timer_node *node) {
	list->tail->next = node;
	list->tail = node;
	node->next=0;
}

// 添加定时器节点
static void
add_node(struct timer *T,struct timer_node *node) {
	uint32_t time=node->expire;
	uint32_t current_time=T->time;

	// 把节点的过期触发事件和当前时间作比较
	
	if ((time|TIME_NEAR_MASK)==(current_time|TIME_NEAR_MASK)) {
		// 比较高24位，如果高24位相同，把低8位作为索引，放入near数组对应slot中
		link(&T->near[time&TIME_NEAR_MASK],node);
	} else {
		// 如果高24位不同
		int i;
		/*
			左移6位  i=0 比较高18位，如果高18位相同，把低8-14位右移8位到低6位作为索引，放入t[0]数组对应slot中
			左移12位 i=1 比较高12位，如果高12位相同，把低14-20位右移14位到低6位作为索引，放入t[1]数组对应slot中
			左移18位 i=2 比较高6位，如果高6位相同，把低20-26位右移20位到低6位作为索引，放入t[2]数组对应slot中
			左移24位 i=3 把低26-32位右移26位到低6位作为索引，放入t[3]数组对应slot中	
		*/
		uint32_t mask=TIME_NEAR << TIME_LEVEL_SHIFT;
		for (i=0;i<3;i++) {
			if ((time|(mask-1))==(current_time|(mask-1))) {
				break;
			}
			mask <<= TIME_LEVEL_SHIFT;
		}

		link(&T->t[i][((time>>(TIME_NEAR_SHIFT + i*TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)],node);	
	}
}

static void
timer_add(struct timer *T,void *arg,size_t sz,int time) {
	// 柔性结构体，除了分配内存给time_node，额外分配sz大小的内存，存放time_event数据
	struct timer_node *node = (struct timer_node *)skynet_malloc(sizeof(*node)+sz);
	// 复制timer_event数据到尾部内存段
	memcpy(node+1,arg,sz);

	SPIN_LOCK(T);

		node->expire=time+T->time;
		add_node(T,node);

	SPIN_UNLOCK(T);
}

// 移动链表
static void
move_list(struct timer *T, int level, int idx) {
	// 清空链表，取出第一个节点，依次遍历下一个节点，添加回链表数组中
	struct timer_node *current = link_clear(&T->t[level][idx]);
	while (current) {
		struct timer_node *temp=current->next;
		add_node(T,current);
		current=temp;
	}
}

// 定时器偏移
// 一个skynet单位时间偏移一次
static void
timer_shift(struct timer *T) {
	int mask = TIME_NEAR;
	// 时间+1
	uint32_t ct = ++T->time;
	if (ct == 0) {
		// 如果32位均为0
		// 无符号32位整数溢出，编译器会和2<<32取余，所以可以为0
		move_list(T, 3, 0);
	} else {
		uint32_t time = ct >> TIME_NEAR_SHIFT;
		int i=0;

		/*
			i=0 	如果低8位为0	右移8位取6位slot索引值
			i=1		如果低14位为0 	右移14位取6位slot索引值	
			i=2		如果低20位为0 	右移20位取6位slot索引值	
			i=3		如果低26位为0 	右移26位取6位slot索引值	
		*/
		while ((ct & (mask-1))==0) {
			int idx=time & TIME_LEVEL_MASK;
			if (idx!=0) {
				// 如果索引不为0，移动链表                                                                  
				move_list(T, i, idx);
				break;				
			}
			// 如果索引为0，继续移位
			mask <<= TIME_LEVEL_SHIFT;
			time >>= TIME_LEVEL_SHIFT;
			++i;
		}
	}
}

// 分发定时器列表消息
static inline void
dispatch_list(struct timer_node *current) {
	do {
		// 获取time_event数据
		struct timer_event * event = (struct timer_event *)(current+1);
		// 创建回应消息
		struct skynet_message message;
		message.source = 0;
		message.session = event->session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;
	
		// 发送回应消息
		skynet_context_push(event->handle, &message);

		// 释放当前节点
		struct timer_node * temp = current;
		// 遍历下一节点
		current=current->next;
		skynet_free(temp);	
	} while (current);
}

// 运行定时器
static inline void
timer_execute(struct timer *T) {
	// 取低八位索引
	int idx = T->time & TIME_NEAR_MASK;
	
	while (T->near[idx].head.next) {
		struct timer_node *current = link_clear(&T->near[idx]);
		SPIN_UNLOCK(T);
		// dispatch_list don't need lock T
		dispatch_list(current);
		SPIN_LOCK(T);
	}
}

// 定期器更新
static void 
timer_update(struct timer *T) {
	SPIN_LOCK(T);

	// try to dispatch timeout 0 (rare condition)
	timer_execute(T);

	// shift time first, and then dispatch timer message
	timer_shift(T);

	timer_execute(T);

	SPIN_UNLOCK(T);
}

// 创建一个定时器
static struct timer *
timer_create_timer() {
	struct timer *r=(struct timer *)skynet_malloc(sizeof(struct timer));
	memset(r,0,sizeof(*r));

	int i,j;

	// TIME_NEAR 2 << 8
	for (i=0;i<TIME_NEAR;i++) {
		link_clear(&r->near[i]);
	}

	// TIME_LEVEL 2 << 6
	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			link_clear(&r->t[i][j]);
		}
	}

	SPIN_INIT(r)

	r->current = 0;

	return r;
}

// skynet.timeout > cmd_timeout > skynet_timeout
int
skynet_timeout(uint32_t handle, int time, int session) {
	if (time <= 0) {
		// 如果time为0，不注册定时器，直接发送回应消息
		struct skynet_message message;
		message.source = 0;
		message.session = session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		if (skynet_context_push(handle, &message)) {
			return -1;
		}
	} else {
		// 如果time不为0，注册定时器事件
		struct timer_event event;
		event.handle = handle;
		event.session = session;
		timer_add(TI, &event, sizeof(event), time);
	}

	return session;
}

// centisecond: 1/100 second
// skynet单位时间为1/100秒
// sec 当前时刻的UTC秒数
// cs 秒数外剩余时间的skynet单位时间数，系统中作为 开始计时后的累计skynet单位时间数，运行过程中通过计算getime()差值保持增长
static void
systime(uint32_t *sec, uint32_t *cs) {
#if !defined(__APPLE__)
	// tv_sec 秒
	// tv_nsec 纳秒 1/1000000000 秒
	struct timespec ti;
	clock_gettime(CLOCK_REALTIME, &ti);
	*sec = (uint32_t)ti.tv_sec;
	*cs = (uint32_t)(ti.tv_nsec / 10000000);
#else
	// tv_sec 秒
	// tv_usec 微秒 1/100000 秒
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*sec = tv.tv_sec;
	*cs = tv.tv_usec / 10000;
#endif
}

// 当前时刻的UTC时间（以skynet单位时间为最小精度）
static uint64_t
gettime() {
	uint64_t t;
#if !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_MONOTONIC, &ti);
	t = (uint64_t)ti.tv_sec * 100;
	t += ti.tv_nsec / 10000000;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = (uint64_t)tv.tv_sec * 100;
	t += tv.tv_usec / 10000;
#endif
	return t;
}

// 时间更新主函数，在thread_timer中被定时器线程循环调用
void
skynet_updatetime(void) {
	uint64_t cp = gettime();
	if(cp < TI->current_point) {
		skynet_error(NULL, "time diff error: change from %lld to %lld", cp, TI->current_point);
		TI->current_point = cp;
	} else if (cp != TI->current_point) {
		uint32_t diff = (uint32_t)(cp - TI->current_point);
		TI->current_point = cp;
		// 根据gettime校正current表示的运行累计skynet单位时间数
		TI->current += diff;
		int i;
		for (i=0;i<diff;i++) {
			// 每个skynet单位时间都执行定时器更新逻辑
			timer_update(TI);
		}
	}
}

// skynet系统启动时间，即启动时的UTC时间秒数
uint32_t
skynet_starttime(void) {
	return TI->starttime;
}

// skynet系统当前时间，即启动后的skynet单位时间数
uint64_t 
skynet_now(void) {
	return TI->current;
}

void 
skynet_timer_init(void) {
	// 创建定时器
	TI = timer_create_timer();
	uint32_t current = 0;
	systime(&TI->starttime, &current);
	// 当前运行累计skynet单位时间数，即开始计时后的累计skynet单位时间数
	TI->current = current;
	// 当前以skynet单位时间精度的UTC时间
	TI->current_point = gettime();
}

