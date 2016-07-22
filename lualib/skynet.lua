local c = require "skynet.core"
local tostring = tostring
local tonumber = tonumber
local coroutine = coroutine
local assert = assert
local pairs = pairs
local pcall = pcall

local profile = require "profile"

local coroutine_resume = profile.resume
local coroutine_yield = profile.yield

local proto = {}
-- 消息类型，与skynet.h中的宏相对应
-- 相当于0-255个端口
local skynet = {
	-- read skynet.h
	PTYPE_TEXT = 0,
	PTYPE_RESPONSE = 1,
	PTYPE_MULTICAST = 2,
	PTYPE_CLIENT = 3,
	PTYPE_SYSTEM = 4,
	PTYPE_HARBOR = 5,
	PTYPE_SOCKET = 6,
	PTYPE_ERROR = 7,
	PTYPE_QUEUE = 8,	-- used in deprecated mqueue, use skynet.queue instead
	PTYPE_DEBUG = 9,
	PTYPE_LUA = 10,
	PTYPE_SNAX = 11,
}

-- code cache
skynet.cache = require "skynet.codecache"

-- 注册消息协议
-- class.name:"text","response","client","lua"...
-- class.id:PTYPE_TEXT,PTYPE_RESPONSE,PTYPE_CLIENT,PTYPE_LUA...
-- class.pack:skynet.pack...
-- class.unpack:skynet.unpack...
function skynet.register_protocol(class)
	local name = class.name
	local id = class.id
	assert(proto[name] == nil)
	assert(type(name) == "string" and type(id) == "number" and id >=0 and id <=255)
	proto[name] = class
	proto[id] = class
end

local session_id_coroutine = {}			-- session -> coroutine
local session_coroutine_id = {}			-- coroutine -> session
local session_coroutine_address = {}	-- coroutine -> service
local session_response = {}				-- coroutine -> true/false/nil
local unresponse = {}					-- response -> true/false/nil

local wakeup_session = {}				-- coroutine -> true/nil
local sleep_session = {}				-- coroutine -> true/nil

local watching_service = {}				-- service -> ref
local watching_session = {}				-- session -> service
local dead_service = {}					-- service -> true/nil
local error_queue = {}					-- index -> session
local fork_queue = {}					-- index -> co

-- suspend is function
local suspend

local function string_to_handle(str)
	return tonumber("0x" .. string.sub(str , 2))
end

----- monitor exit

-- 处理错误的会话消息
local function dispatch_error_queue()
	local session = table.remove(error_queue,1)
	if session then
		-- 获取session对应的协程
		local co = session_id_coroutine[session]
		-- 清理session_id_coroutine标识
		session_id_coroutine[session] = nil
		-- yield_call 返回false
		return suspend(co, coroutine_resume(co, false))
	end
end

-- 错误分发函数
local function _error_dispatch(error_session, error_source)
	if error_session == 0 then
		-- service is down
		--  Don't remove from watching_service , because user may call dead service
		if watching_service[error_source] then
			dead_service[error_source] = true
		end
		for session, srv in pairs(watching_session) do
			if srv == error_source then
				table.insert(error_queue, session)
			end
		end
	else
		-- capture an error for error_session
		if watching_session[error_session] then
			table.insert(error_queue, error_session)
		end
	end
end

-- coroutine reuse

-- 协程池
local coroutine_pool = setmetatable({}, { __mode = "kv" })

-- 创建协程/复用协程
local function co_create(f)
	-- 从协程池取出协程
	local co = table.remove(coroutine_pool)
	if co == nil then
		-- 如果协程池没有可用协程，执行coroutine.create创建新的协程
		-- skynet.start->skynet.timeout()->co_create 第一次创建新的协程
		co = coroutine.create(function(...)
			-- 创建后的协程被唤醒后，调用co_create传入的方法，以coroutine_resume传入参数执行
			-- 第一次唤醒在raw_dispatch_message方法，收到定时器回应消息时执行suspend(co, coroutine_resume(co, true, msg, sz))
			-- 第一次调用co_create传入的方法，实际上就是skynet.init_service，参数是 true, msg, sz
			f(...)
			while true do
				f = nil
				-- 协程重用
				coroutine_pool[#coroutine_pool+1] = co
				-- 运行的协程被挂起，传递参数true, "EXIT"给suspend(co, coroutine_resume(co, ...))，执行suspend(co, true, "EXIT")
				-- 协程被复用的时候，执行coroutine_resume(co, f)，唤醒协程获得新的方法参数
				f = coroutine_yield "EXIT"
				-- 此处只需返回协程，不需要真正的执行，所以暂时先挂起等待唤醒
				-- 当执行coroutine_resume，唤醒协程并传递参数，方法真正开始执行
				f(coroutine_yield())
			end
		end)
	else
		-- 如果协程池有可用协程，重新唤醒协程传入新的方法参数
		coroutine_resume(co, f)
	end
	return co
end

-- 处理需要唤醒的协程
local function dispatch_wakeup()
	-- 弹出一个需要唤醒协程
	local co = next(wakeup_session)
	if co then
		wakeup_session[co] = nil
		local session = sleep_session[co]
		if session then
			-- 中途唤醒的协程，为了避免定时器触发时，正确处理回调消息，此处修改标识为BREAK，raw_dispatch_message中修改标识为nil
			session_id_coroutine[session] = "BREAK"
			-- 唤醒skynet.sleep中挂起的协程，并传递参数false, "BREAK"
			return suspend(co, coroutine_resume(co, false, "BREAK"))
		end
	end
end

-- 释放服务引用计数
-- 在执行闭包和EXIT处理中减少引用计数
-- 在生成闭包和处理非RESPONSE时增加引用计数
local function release_watching(address)
	local ref = watching_service[address]
	if ref then
		ref = ref - 1
		if ref > 0 then
			watching_service[address] = ref
		else
			watching_service[address] = nil
		end
	end
end

-- suspend is local function
-- suspend处理yield挂起的协程
function suspend(co, result, command, param, size)
	if not result then
		-- 如果协程唤醒失败
		local session = session_coroutine_id[co]
		if session then -- coroutine may fork by others (session is nil)
			local addr = session_coroutine_address[co]
			if session ~= 0 then
				-- only call response error
				c.send(addr, skynet.PTYPE_ERROR, session, "")
			end
			session_coroutine_id[co] = nil
			session_coroutine_address[co] = nil
		end
		error(debug.traceback(co,tostring(command)))
	end
	if command == "CALL" then
		-- yield_call中coroutine_yield("CALL", session)挂起协程，注册session_id_coroutine标识
		session_id_coroutine[param] = co
	elseif command == "SLEEP" then
		-- skynet.sleep中coroutine_yield("SLEEP", session)挂起协程，注册session_id_coroutine和sleep_session标识
		session_id_coroutine[param] = co
		sleep_session[co] = param
	elseif command == "RETURN" then
		-- skynet.ret中coroutine_yield("RETURN", msg, sz)挂起协程，注册session_id_coroutine和session_coroutine_address以及session_response标识，发送回应消息给对方服务
		local co_session = session_coroutine_id[co]
		local co_address = session_coroutine_address[co]
		if param == nil or session_response[co] then
			error(debug.traceback(co))
		end
		session_response[co] = true
		local ret
		if not dead_service[co_address] then
			ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, param, size) ~= nil
			if not ret then
				-- If the package is too large, returns nil. so we should report error back
				c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
			end
		elseif size ~= nil then
			c.trash(param, size)
			ret = false
		end
		-- 非阻塞，继续运行协程
		return suspend(co, coroutine_resume(co, ret))
	elseif command == "RESPONSE" then
		-- skynet.response中coroutine_yield("RESPONSE", pack)挂起协程，注册session_id_coroutine和session_coroutine_address以及session_response、unresponse标识，生成闭包发给对方服务
		local co_session = session_coroutine_id[co]
		local co_address = session_coroutine_address[co]
		if session_response[co] then
			error(debug.traceback(co))
		end
		local f = param
		-- 闭包
		local function response(ok, ...)
			if ok == "TEST" then
				if dead_service[co_address] then
					release_watching(co_address)
					unresponse[response] = nil
					f = false
					return false
				else
					return true
				end
			end
			if not f then
				if f == false then
					f = nil
					return false
				end
				error "Can't response more than once"
			end

			local ret
			if not dead_service[co_address] then
				if ok then
					ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, f(...)) ~= nil
					if not ret then
						-- If the package is too large, returns false. so we should report error back
						c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
					end
				else
					ret = c.send(co_address, skynet.PTYPE_ERROR, co_session, "") ~= nil
				end
			else
				ret = false
			end
			release_watching(co_address)
			unresponse[response] = nil
			f = nil
			return ret
		end
		-- 增加服务引用计数
		watching_service[co_address] = watching_service[co_address] + 1
		session_response[co] = true
		unresponse[response] = true
		-- 非阻塞，继续运行协程
		return suspend(co, coroutine_resume(co, response))
	elseif command == "EXIT" then
		-- coroutine exit
		-- co_create中coroutine_yield "EXIT"挂起协程，释放引用计数，清理注册标识
		local address = session_coroutine_address[co]
		release_watching(address)
		session_coroutine_id[co] = nil
		session_coroutine_address[co] = nil
		session_response[co] = nil
	elseif command == "QUIT" then
		-- skynet.exit中coroutine_yield "QUIT"挂起协程
		-- service exit
		return
	elseif command == "USER" then
		-- See skynet.coutine for detail
		error("Call skynet.coroutine.yield out of skynet.coroutine.resume\n" .. debug.traceback(co))
	elseif command == nil then
		-- debug trace
		return
	else
		error("Unknown command : " .. command .. "\n" .. debug.traceback(co))
	end
	-- 处理需要唤醒的协程
	dispatch_wakeup()
	-- 处理错误的会话消息
	dispatch_error_queue()
end

-- 定时触发
function skynet.timeout(ti, func)
	-- 启动定时器
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	-- 创建新协程
	local co = co_create(func)
	assert(session_id_coroutine[session] == nil)
	-- 注册session_id_coroutine
	session_id_coroutine[session] = co
end

-- 休眠协程
function skynet.sleep(ti)
	-- 启动定时器
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	-- 挂起当前协程，进入suspend(co, true, "SLEEP", session)，注册休眠标识
	-- 1. 定时器触发，在回应消息处理中唤醒协程，返回true
	-- 2. 中途被唤醒，执行dispatch_wakeup方法，返回false, BREAK
	local succ, ret = coroutine_yield("SLEEP", session)
	-- 清理休眠标识
	sleep_session[coroutine.running()] = nil
	if succ then
		return
	end
	if ret == "BREAK" then
		return "BREAK"
	else
		error(ret)
	end
end

-- 挂起协程
function skynet.yield()
	return skynet.sleep(0)
end

-- 挂起协程
function skynet.wait(co)
	-- 不启动定时器
	local session = c.genid()
	local ret, msg = coroutine_yield("SLEEP", session)
	co = co or coroutine.running()
	sleep_session[co] = nil
	session_id_coroutine[session] = nil
end

local self_handle
-- 获取当前服务地址
function skynet.self()
	if self_handle then
		return self_handle
	end
	self_handle = string_to_handle(c.command("REG"))
	return self_handle
end

-- 查询当前服务地址
function skynet.localname(name)
	local addr = c.command("QUERY", name)
	if addr then
		return string_to_handle(addr)
	end
end

skynet.now = c.now

local starttime

-- 获取skynet进程启动的UTC时间，以秒为单位
function skynet.starttime()
	if not starttime then
		starttime = c.intcommand("STARTTIME")
	end
	return starttime
end

-- 获取当前的UTC时间，以秒为单位
function skynet.time()
	return skynet.now()/100 + (starttime or skynet.starttime())
end

function skynet.exit()
	fork_queue = {}	-- no fork coroutine can be execute after skynet.exit
	skynet.send(".launcher","lua","REMOVE",skynet.self(), false)
	-- report the sources that call me
	for co, session in pairs(session_coroutine_id) do
		local address = session_coroutine_address[co]
		if session~=0 and address then
			c.redirect(address, 0, skynet.PTYPE_ERROR, session, "")
		end
	end
	-- 已生成闭包，但没有执行，执行但不发送消息
	for resp in pairs(unresponse) do
		resp(false)
	end
	-- report the sources I call but haven't return
	local tmp = {}
	for session, address in pairs(watching_session) do
		tmp[address] = true
	end
	for address in pairs(tmp) do
		c.redirect(address, 0, skynet.PTYPE_ERROR, 0, "")
	end
	c.command("EXIT")
	-- quit service
	coroutine_yield "QUIT"
end

function skynet.getenv(key)
	return (c.command("GETENV",key))
end

function skynet.setenv(key, value)
	c.command("SETENV",key .. " " ..value)
end

function skynet.send(addr, typename, ...)
	local p = proto[typename]
	return c.send(addr, p.id, 0 , p.pack(...))
end

skynet.genid = assert(c.genid)

skynet.redirect = function(dest,source,typename,...)
	return c.redirect(dest, source, proto[typename].id, ...)
end

skynet.pack = assert(c.pack)
skynet.packstring = assert(c.packstring)
skynet.unpack = assert(c.unpack)
skynet.tostring = assert(c.tostring)
skynet.trash = assert(c.trash)

-- 挂起协程，等待消息
local function yield_call(service, session)
	watching_session[session] = service
	--[[
		挂起协程：运行的协程被挂起传递参数，执行suspend(co, true, "CALL", session)，注册session_id_coroutine等待回应消息
		唤醒协程：有回应消息来触发回调函数，在raw_dispatch_message中处理PTYPE_RESPONSE分支调用suspend(co, coroutine_resume(co, true, msg, sz))，resume唤醒协程同时传递参数给yield
	]]--
	local succ, msg, sz = coroutine_yield("CALL", session)
	watching_session[session] = nil
	if not succ then
		error "call failed"
	end
	return msg,sz
end

-- 向一个服务发送请求
function skynet.call(addr, typename, ...)
	local p = proto[typename]
	-- 发送请求消息，返回会话session
	local session = c.send(addr, p.id , nil , p.pack(...))
	if session == nil then
		error("call to invalid address " .. skynet.address(addr))
	end
	-- 对接收到的消息进行解包返回
	return p.unpack(yield_call(addr, session))
end

-- 向一个服务发送请求
function skynet.rawcall(addr, typename, msg, sz)
	local p = proto[typename]
	local session = assert(c.send(addr, p.id , nil , msg, sz), "call to invalid address")
	-- 直接返回接受的消息
	return yield_call(addr, session)
end

-- 发送回应消息
function skynet.ret(msg, sz)
	msg = msg or ""
	return coroutine_yield("RETURN", msg, sz)
end

-- 生成回应闭包，执行闭包发送回应消息
function skynet.response(pack)
	pack = pack or skynet.pack
	return coroutine_yield("RESPONSE", pack)
end

-- 发送回应消息
-- 对消息内容序列化
function skynet.retpack(...)
	return skynet.ret(skynet.pack(...))
end

-- 唤醒协程
function skynet.wakeup(co)
	if sleep_session[co] and wakeup_session[co] == nil then
		wakeup_session[co] = true
		return true
	end
end

-- 注册特定类型的消息分发函数
--[[
	示例代码如下：
	skynet.dispatch("lua", function(_,_, command, ...)
	    local f = CMD[command]
	    skynet.ret(skynet.pack(f(...)))
	end)
]]--
function skynet.dispatch(typename, func)
	local p = proto[typename]
	if func then
		local ret = p.dispatch
		p.dispatch = func
		return ret
	else
		return p and p.dispatch
	end
end

local function unknown_request(session, address, msg, sz, prototype)
	skynet.error(string.format("Unknown request (%s): %s", prototype, c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_request(unknown)
	local prev = unknown_request
	unknown_request = unknown
	return prev
end

local function unknown_response(session, address, msg, sz)
	skynet.error(string.format("Response message : %s" , c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_response(unknown)
	local prev = unknown_response
	unknown_response = unknown
	return prev
end

-- fork一个新协程处理逻辑
function skynet.fork(func,...)
	local args = table.pack(...)
	local co = co_create(function()
		func(table.unpack(args,1,args.n))
	end)
	table.insert(fork_queue, co)
	return co
end

local function raw_dispatch_message(prototype, msg, sz, session, source)
	-- skynet.PTYPE_RESPONSE = 1, read skynet.h
	if prototype == 1 then
		-- PTYPE_RESPONSE 返回消息
		-- PTYPE_RESPONSE 有以下几种情况
		-- 1. skynet.timeout(skynet.lua)->skynet_timeout(skynet_time.c)，定期器触发时会发送PTYPE_RESPONSE消息给源服务
		-- 2. skynet.ret->suspend("RETRUN")，正常消息回应会发送PTYPE_RESPONSE消息给源服务
		-- 3. skynet.response->suspend("RESPONSE")，闭包消息回应会发送PTYPE_RESPONSE消息给源服务

		-- 获取session对应的coroutine
		local co = session_id_coroutine[session]
		if co == "BREAK" then
			session_id_coroutine[session] = nil
		elseif co == nil then
			unknown_response(session, source, msg, sz)
		else
			-- 清理session对应的coroutine
			session_id_coroutine[session] = nil
			-- 运行协程，等待挂起
			-- 一个服务，第一个回应消息来自于timer
			-- 其他情况，true, msg, sz 返回给 yield_call
			suspend(co, coroutine_resume(co, true, msg, sz))
		end
	else
		-- 其他类型请求消息
		local p = proto[prototype]
		if p == nil then
			if session ~= 0 then
				c.send(source, skynet.PTYPE_ERROR, session, "")
			else
				unknown_request(session, source, msg, sz, prototype)
			end
			return
		end
		local f = p.dispatch
		if f then
			-- 增加服务的引用计数
			local ref = watching_service[source]
			if ref then
				watching_service[source] = ref + 1
			else 
				watching_service[source] = 1
			end
			-- 创建协程处理消息
			local co = co_create(f)
			-- 记录协程对应的信息
			session_coroutine_id[co] = session
			session_coroutine_address[co] = source
			-- 运行协程，把session, source, p.unpack(msg,sz)作为参数传入消息分发函数
			-- 运行dispatch，如果遇到skynet.ret/skynet.response，挂起协程，suspend接受参数继续执行RETURN/RESPONSE逻辑
			suspend(co, coroutine_resume(co, session,source, p.unpack(msg,sz)))
		else
			unknown_request(session, source, msg, sz, proto[prototype].name)
		end
	end
end

-- 消息分发函数
-- 在sknyet.start中，通过c.callback注册 
function skynet.dispatch_message(...)
	-- 消息分发
	local succ, err = pcall(raw_dispatch_message,...)
	-- fork处理
	while true do
		local key,co = next(fork_queue)
		if co == nil then
			break
		end
		fork_queue[key] = nil
		local fork_succ, fork_err = pcall(suspend,co,coroutine_resume(co))
		if not fork_succ then
			if succ then
				succ = false
				err = tostring(fork_err)
			else
				err = tostring(err) .. "\n" .. tostring(fork_err)
			end
		end
	end
	assert(succ, tostring(err))
end

-- 启动新的服务
function skynet.newservice(name, ...)
	return skynet.call(".launcher", "lua" , "LAUNCH", "snlua", name, ...)
end

-- 启动单例服务
function skynet.uniqueservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GLAUNCH", ...))
	else
		return assert(skynet.call(".service", "lua", "LAUNCH", global, ...))
	end
end

-- 查询单例服务
function skynet.queryservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GQUERY", ...))
	else
		return assert(skynet.call(".service", "lua", "QUERY", global, ...))
	end
end

-- 获取字符串地址
function skynet.address(addr)
	if type(addr) == "number" then
		return string.format(":%08x",addr)
	else
		return tostring(addr)
	end
end

-- 获得服务所属节点
function skynet.harbor(addr)
	return c.harbor(addr)
end

skynet.error = c.error

----- register protocol
-- 默认注册了PTYPE_LUA、PTYPE_RESPONSE、PTYPE_ERROR三类消息协议
do
	local REG = skynet.register_protocol

	REG {
		name = "lua",
		id = skynet.PTYPE_LUA,
		pack = skynet.pack,
		unpack = skynet.unpack,
	}

	REG {
		name = "response",
		id = skynet.PTYPE_RESPONSE,
	}

	REG {
		name = "error",
		id = skynet.PTYPE_ERROR,
		unpack = function(...) return ... end,
		dispatch = _error_dispatch,
	}
end

local init_func = {}

function skynet.init(f, name)
	assert(type(f) == "function")
	if init_func == nil then
		f()
	else
		table.insert(init_func, f)
		if name then
			assert(type(name) == "string")
			assert(init_func[name] == nil)
			init_func[name] = f
		end
	end
end

local function init_all()
	local funcs = init_func
	init_func = nil
	if funcs then
		for _,f in ipairs(funcs) do
			f()
		end
	end
end

local function ret(f, ...)
	f()
	return ...
end

local function init_template(start, ...)
	init_all()
	init_func = {}
	return ret(init_all, start(...))
end

function skynet.pcall(start, ...)
	return xpcall(init_template, debug.traceback, start, ...)
end

-- 初始服务
function skynet.init_service(start)
	-- 执行入口函数
	local ok, err = skynet.pcall(start)
	if not ok then
		-- 如果执行失败，发送错误消息，并退出服务
		skynet.error("init service failed: " .. tostring(err))
		skynet.send(".launcher","lua", "ERROR")
		skynet.exit()
	else
		-- 向launcher服务发送LAUNCHOK请求
		skynet.send(".launcher","lua", "LAUNCHOK")
	end
end

-- skynet服务入口函数
function skynet.start(start_func)
	-- 注册服务回调函数
	c.callback(skynet.dispatch_message)
	-- 启动服务中的协程，执行初始服务方法
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

-- 返回服务是否无限循环
function skynet.endless()
	return c.command("ENDLESS")~=nil
end

-- 返回当前服务消息数量
function skynet.mqlen()
	return c.intcommand "MQLEN"
end

function skynet.task(ret)
	local t = 0
	for session,co in pairs(session_id_coroutine) do
		if ret then
			ret[session] = debug.traceback(co)
		end
		t = t + 1
	end
	return t
end

function skynet.term(service)
	return _error_dispatch(0, service)
end

-- 设置内存限制字节数
function skynet.memlimit(bytes)
	debug.getregistry().memlimit = bytes
	skynet.memlimit = nil	-- set only once
end

-- Inject internal debug framework
local debug = require "skynet.debug"
debug.init(skynet, {
	dispatch = skynet.dispatch_message,
	suspend = suspend,
})

return skynet
