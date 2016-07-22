local skynet = require "skynet"
local c = require "skynet.core"

-- 启动服务
function skynet.launch(...)
	-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_launch(skynet_server.c)->skynet_context_new(skynet_server.c)
	local addr = c.command("LAUNCH", table.concat({...}," "))
	if addr then
		return tonumber("0x" .. string.sub(addr , 2))
	end
end

-- 杀死服务
function skynet.kill(name)
	if type(name) == "number" then
		-- 移除未完成初始化的服务实例
		skynet.send(".launcher","lua","REMOVE",name, true)
		name = skynet.address(name)
	end
	-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_kill(skynet_server.c)->handle_exit(skynet_server.c)
	c.command("KILL",name)
end

-- 中止所有服务
function skynet.abort()
	-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_abort(skynet_server.c)->skynet_handle_retireall(skynet_server.c)
	c.command("ABORT")
end


local function globalname(name, handle)
	local c = string.sub(name,1,1)
	assert(c ~= ':')
	if c == '.' then
		-- .开头，本地节点服务
		return false
	end

	assert(#name <= 16)	-- GLOBALNAME_LENGTH is 16, defined in skynet_harbor.h
	assert(tonumber(name) == nil)	-- global name can't be number

	local harbor = require "skynet.harbor"

	harbor.globalname(name, handle)

	return true
end

-- 给当前服务注册一个别名
function skynet.register(name)
	if not globalname(name) then
		-- .开头
		-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_reg(skynet_server.c)->skynet_handle_namehandle(skynet_server.c)
		c.command("REG", name)
	end
end

-- 给handle服务注册一个别名
function skynet.name(name, handle)
	if not globalname(name, handle) then
		-- lcommand(lua-skynet.c)->skynet_command(skynet_server.c)->cmd_name(skynet_server.c)->skynet_handle_namehandle(skynet_server.c)
		c.command("NAME", name .. " " .. skynet.address(handle))
	end
end

local dispatch_message = skynet.dispatch_message

function skynet.forward_type(map, start_func)
	c.callback(function(ptype, msg, sz, ...)
		local prototype = map[ptype]
		if prototype then
			dispatch_message(prototype, msg, sz, ...)
		else
			dispatch_message(ptype, msg, sz, ...)
			c.trash(msg, sz)
		end
	end, true)
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.filter(f ,start_func)
	c.callback(function(...)
		dispatch_message(f(...))
	end)
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.monitor(service, query)
	local monitor
	if query then
		monitor = skynet.queryservice(true, service)
	else
		monitor = skynet.uniqueservice(true, service)
	end
	assert(monitor, "Monitor launch failed")
	c.command("MONITOR", string.format(":%08x", monitor))
	return monitor
end

return skynet
