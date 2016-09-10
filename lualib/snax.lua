local skynet = require "skynet"
local snax_interface = require "snax.interface"

local snax = {}
local typeclass = {}

local interface_g = skynet.getenv("snax_interface_g")
local G = interface_g and require (interface_g) or { require = function() end }
interface_g = nil

skynet.register_protocol {
	name = "snax",
	id = skynet.PTYPE_SNAX,
	pack = skynet.pack,
	unpack = skynet.unpack,
}


--[[ 
	遍历name对应snax模块方法，返回[group][name]->id形式的数据，便于获取方法id向snaxd服务传递

	{
		{ id1, group1, name1, function1 },
		{ id2, group2, name2, function2 },
	}
	
	>>>

	{
		name = name,
		accept = { id1, id2, id3, ... },
		response = { id4, id5, id6, ... },
		system = { id7, id8, id9, ... },
	}

]]
function snax.interface(name)
	if typeclass[name] then
		return typeclass[name]
	end

	local si = snax_interface(name, G)

	local ret = {
		name = name,
		accept = {},
		response = {},
		system = {},
	}

	for _,v in ipairs(si) do
		local id, group, name, f = table.unpack(v)
		ret[group][name] = id
	end

	typeclass[name] = ret
	return ret
end

local meta = { __tostring = function(v) return string.format("[%s:%x]", v.type, v.handle) end}

local skynet_send = skynet.send
local skynet_call = skynet.call

-- type, snax.interface 返回的数据
-- handle, snaxd服务地址
local function gen_post(type, handle)
	return setmetatable({} , {
		__index = function( t, k )
			local id = type.accept[k]
			if not id then
				error(string.format("post %s:%s no exist", type.name, k))
			end
			return function(...)
				skynet_send(handle, "snax", id, ...)
			end
		end })
end

-- type, snax.interface 返回的数据
-- handle, snaxd服务地址
local function gen_req(type, handle)
	return setmetatable({} , {
		__index = function( t, k )
			local id = type.response[k]
			if not id then
				error(string.format("request %s:%s no exist", type.name, k))
			end
			return function(...)
				return skynet_call(handle, "snax", id, ...)
			end
		end })
end

-- 打包snax对象
local function wrapper(handle, name, type)
	return setmetatable ({
		post = gen_post(type, handle),	-- accept方法接口
		req = gen_req(type, handle),	-- response方法接口
		type = name,					-- snax服务名
		handle = handle,				-- snax服务地址
		}, meta)
end

-- handle->wrapper，缓存服务对应的打包对象
local handle_cache = setmetatable( {} , { __mode = "kv" } )

-- 生成snaxd服务
function snax.rawnewservice(name, ...)
	local t = snax.interface(name)
	local handle = skynet.newservice("snaxd", name)
	assert(handle_cache[handle] == nil)
	if t.system.init then
		-- 如果有init方法，执行后再返回
		skynet.call(handle, "snax", t.system.init, ...)
	end
	return handle
end

-- 绑定服务handle和wrapper对象
function snax.bind(handle, type)
	local ret = handle_cache[handle]
	if ret then
		assert(ret.type == type)
		return ret
	end
	local t = snax.interface(type)
	ret = wrapper(handle, type, t)
	handle_cache[handle] = ret
	return ret
end

-- 生成snaxd服务，返回snax对象
function snax.newservice(name, ...)
	local handle = snax.rawnewservice(name, ...)
	return snax.bind(handle, name)
end

local function service_name(global, name, ...)
	if global == true then
		return name
	else
		return global
	end
end

function snax.uniqueservice(name, ...)
	local handle = assert(skynet.call(".service", "lua", "LAUNCH", "snaxd", name, ...))
	return snax.bind(handle, name)
end

function snax.globalservice(name, ...)
	local handle = assert(skynet.call(".service", "lua", "GLAUNCH", "snaxd", name, ...))
	return snax.bind(handle, name)
end

function snax.queryservice(name)
	local handle = assert(skynet.call(".service", "lua", "QUERY", "snaxd", name))
	return snax.bind(handle, name)
end

function snax.queryglobal(name)
	local handle = assert(skynet.call(".service", "lua", "GQUERY", "snaxd", name))
	return snax.bind(handle, name)
end

function snax.kill(obj, ...)
	local t = snax.interface(obj.type)
	skynet_call(obj.handle, "snax", t.system.exit, ...)
end

-- 返回snax服务handle对应的打包对象
function snax.self()
	return snax.bind(skynet.self(), SERVICE_NAME)
end

function snax.exit(...)
	snax.kill(snax.self(), ...)
end

local function test_result(ok, ...)
	-- hotfix.lua 返回 pcall(inject, funcs, source, ...)
	if ok then
		return ...
	else
		error(...)
	end
end

function snax.hotfix(obj, source, ...)
	local t = snax.interface(obj.type)
	return test_result(skynet_call(obj.handle, "snax", t.system.hotfix, source, ...))
end

function snax.printf(fmt, ...)
	skynet.error(string.format(fmt, ...))
end

return snax
