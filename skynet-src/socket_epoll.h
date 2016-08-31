#ifndef poll_socket_epoll_h
#define poll_socket_epoll_h

#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

static bool 
sp_invalid(int efd) {
	return efd == -1;
}

// 创建一个epoll实例，返回文件描述符
static int
sp_create() {
	return epoll_create(1024);
}

// 关闭epoll文件描述符
static void
sp_release(int efd) {
	close(efd);
}

// 把sock加入efd事件循环管理
// 默认开启 EPOLLIN 可读权限
// EPOLLIN，只有当对端有数据写入时才会触发，触发一次后需要不断读取所有数据直到读完EAGAIN为止，否则剩余的数据只有在下次对端有数据写入时才能读到。
// EPOLLOUT，只有在连接时触发一次表示可写。其他时候想触发，必须满足以下两个条件:write写满缓冲区返回EAGAIN，对端读取数据后又可写了
// EPOLLIN，要求读完所有数据，所以要求socket是异步
// EPOLLOUT，只会在内核缓冲区不可写到可写的转变时刻，才会触发一次，所以叫边缘触发
static int 
sp_add(int efd, int sock, void *ud) {
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = ud;
	if (epoll_ctl(efd, EPOLL_CTL_ADD, sock, &ev) == -1) {
		return 1;
	}
	return 0;
}

// 把sock移出efd事件循环管理
static void 
sp_del(int efd, int sock) {
	epoll_ctl(efd, EPOLL_CTL_DEL, sock , NULL);
}

// 修改sock在efd中的权限
static void 
sp_write(int efd, int sock, void *ud, bool enable) {
	struct epoll_event ev;
	ev.events = EPOLLIN | (enable ? EPOLLOUT : 0);
	ev.data.ptr = ud;
	epoll_ctl(efd, EPOLL_CTL_MOD, sock, &ev);
}

// 等待就绪的事件列表
static int 
sp_wait(int efd, struct event *e, int max) {
	struct epoll_event ev[max];
	int n = epoll_wait(efd , ev, max, -1);
	int i;
	for (i=0;i<n;i++) {
		e[i].s = ev[i].data.ptr;
		unsigned flag = ev[i].events;
		e[i].write = (flag & EPOLLOUT) != 0;
		e[i].read = (flag & EPOLLIN) != 0;
	}

	return n;
}

// 设置fd为非阻塞文件描述符
static void
sp_nonblocking(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	if ( -1 == flag ) {
		return;
	}

	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

#endif
