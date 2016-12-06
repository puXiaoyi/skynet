#ifndef SKYNET_IMP_H
#define SKYNET_IMP_H

struct skynet_config {
	int thread;					// 工作线程数
	int harbor;					// harbor开启标识
	int profile;				// 
	const char * daemon;		// 后台运行标识
	const char * module_path; 	// c module地址
	const char * bootstrap;		// 启动服务配置，如 snlua bootstrap
	const char * logger;		// 日志服务配置
	const char * logservice;	// 日志服务地址
};

#define THREAD_WORKER 0			// 工作线程
#define THREAD_MAIN 1			// 主线程
#define THREAD_SOCKET 2			// 网络线程
#define THREAD_TIMER 3			// 定时器线程
#define THREAD_MONITOR 4		// 监视器线程

void skynet_start(struct skynet_config * config);

#endif
