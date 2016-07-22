local skynet = require "skynet"
local core = require "skynet.core"
require "skynet.manager"	-- import manager apis
local string = string

-- 成功创建的服务
local services = {}
local command = {}
-- hanlde -> response，command.LAUNCH中标识，command.LAUNCHOK/command.ERROR中清理
local instance = {} -- for confirm (function command.LAUNCH / command.ERROR / command.LAUNCHOK)

local function handle_to_address(handle)
	return tonumber("0x" .. string.sub(handle , 2))
end

-- 空响应消息
local NORET = {}

function command.LIST()
	local list = {}
	for k,v in pairs(services) do
		list[skynet.address(k)] = v
	end
	return list
end

function command.STAT()
	local list = {}
	for k,v in pairs(services) do
		local ok, stat = pcall(skynet.call,k,"debug","STAT")
		if not ok then
			stat = string.format("ERROR (%s)",v)
		end
		list[skynet.address(k)] = stat
	end
	return list
end

function command.KILL(_, handle)
	handle = handle_to_address(handle)
	skynet.kill(handle)
	local ret = { [skynet.address(handle)] = tostring(services[handle]) }
	services[handle] = nil
	return ret
end

function command.MEM()
	local list = {}
	for k,v in pairs(services) do
		local ok, kb, bytes = pcall(skynet.call,k,"debug","MEM")
		if not ok then
			list[skynet.address(k)] = string.format("ERROR (%s)",v)
		else
			list[skynet.address(k)] = string.format("%.2f Kb (%s)",kb,v)
		end
	end
	return list
end

function command.GC()
	for k,v in pairs(services) do
		skynet.send(k,"debug","GC")
	end
	return command.MEM()
end

-- 移除未完成初始化的服务实例
function command.REMOVE(_, handle, kill)
	services[handle] = nil
	local response = instance[handle]
	if response then
		-- instance is dead
		response(not kill)	-- return nil to caller of newservice, when kill == false
		instance[handle] = nil
	end

	-- don't return (skynet.ret) because the handle may exit
	return NORET
end

-- 启动服务
local function launch_service(service, ...)
	local param = table.concat({...}, " ")
	-- 创建服务实例 manager.lua
	local inst = skynet.launch(service, param)
	-- 生成闭包
	local response = skynet.response()
	if inst then
		-- 记录服务
		services[inst] = service .. " " .. param
		-- 暂存闭包
		instance[inst] = response
	else
		-- 创建服务失败，执行闭包发送错误消息
		response(false)
		return
	end
	return inst
end

-- 启动服务接口，见skynet.lua中skynet.newservice方法
function command.LAUNCH(_, service, ...)
	launch_service(service, ...)
	return NORET
end
-- 启动服务并开启日志
function command.LOGLAUNCH(_, service, ...)
	local inst = launch_service(service, ...)
	if inst then
		-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_logon(skynet_server.c)->skynet_log_open(skynet_server.c)
		core.command("LOGON", skynet.address(inst))
	end
	return NORET
end

function command.ERROR(address)
	-- see serivce-src/service_lua.c
	-- init failed
	local response = instance[address]
	if response then
		response(false)
		instance[address] = nil
	end
	services[address] = nil
	return NORET
end

-- 启动服务成功接口，见skynet.lua中skynet.init_service方法
-- skynet.start中的匿名函数运行完毕发送该请求，包括launcher服务本身也给自己发送此消息
function command.LAUNCHOK(address)
	-- init notice
	local response = instance[address]
	if response then
		response(true, address)
		instance[address] = nil
	end

	return NORET
end

-- for historical reasons, launcher support text command (for C service)

skynet.register_protocol {
	name = "text",
	id = skynet.PTYPE_TEXT,
	unpack = skynet.tostring,
	dispatch = function(session, address , cmd)
		if cmd == "" then
			command.LAUNCHOK(address)
		elseif cmd == "ERROR" then
			command.ERROR(address)
		else
			error ("Invalid text command " .. cmd)
		end
	end,
}

skynet.dispatch("lua", function(session, address, cmd , ...)
	cmd = string.upper(cmd)
	local f = command[cmd]
	if f then
		local ret = f(address, ...)
		if ret ~= NORET then
			skynet.ret(skynet.pack(ret))
		end
	else
		skynet.ret(skynet.pack {"Unknown command"} )
	end
end)

skynet.start(function() end)
