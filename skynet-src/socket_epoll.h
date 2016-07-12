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

// ����һ��epollʵ���������ļ�������
static int
sp_create() {
	return epoll_create(1024);
}

// �ر�epoll�ļ�������
static void
sp_release(int efd) {
	close(efd);
}

// ��sock����efd�¼�ѭ������
// Ĭ�Ͽ��� EPOLLIN �ɶ�Ȩ��
// EPOLLIN��ֻ�е��Զ�������д��ʱ�Żᴥ��������һ�κ���Ҫ���϶�ȡ��������ֱ������EAGAINΪֹ������ʣ�������ֻ�����´ζԶ�������д��ʱ���ܶ�����
// EPOLLOUT��ֻ��������ʱ����һ�α�ʾ��д������ʱ���봥������������������������:writeд������������EAGAIN���Զ˶�ȡ���ݺ��ֿ�д��
// EPOLLIN��Ҫ������������ݣ�����Ҫ��socket���첽
// EPOLLOUT��ֻ�����ں˻���������д����д��ת��ʱ�̣��Żᴥ��һ�Σ����Խб�Ե����
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

// ��sock�Ƴ�efd�¼�ѭ������
static void 
sp_del(int efd, int sock) {
	epoll_ctl(efd, EPOLL_CTL_DEL, sock , NULL);
}

// �޸�sock��efd�е�Ȩ��
static void 
sp_write(int efd, int sock, void *ud, bool enable) {
	struct epoll_event ev;
	ev.events = EPOLLIN | (enable ? EPOLLOUT : 0);
	ev.data.ptr = ud;
	epoll_ctl(efd, EPOLL_CTL_MOD, sock, &ev);
}

// �ȴ��������¼��б�
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

// ����fdΪ�������ļ�������
static void
sp_nonblocking(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	if ( -1 == flag ) {
		return;
	}

	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

#endif
