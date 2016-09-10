local skynet = require "skynet"

-- name, snax模块名
-- G, 环境变量
-- loader, 加载方法
return function (name , G, loader)
	loader = loader or loadfile
	local mainfunc

	local function func_id(id, group)
		-- 保存定义的accept/response方法，避免重复命名
		local tmp = {}
		local function count( _, name, func)
			-- 参数判断
			if type(name) ~= "string" then
				error (string.format("%s method only support string", group))
			end
			if type(func) ~= "function" then
				error (string.format("%s.%s must be function"), group, name)
			end
			-- 重复判断
			if tmp[name] then
				error (string.format("%s.%s duplicate definition", group, name))
			end
			tmp[name] = true
			-- 统一放入func库
			table.insert(id, { #id + 1, group, name, func} )
		end
		return setmetatable({}, { __newindex = count })
	end

	do
		assert(getmetatable(G) == nil)
		assert(G.init == nil)
		assert(G.exit == nil)

		assert(G.accept == nil)
		assert(G.response == nil)
	end

	local temp_global = {}
	local env = setmetatable({} , { __index = temp_global })
	--[[
		id,	group,	name,	func
		------------------------
		1,	system,	init,	init_func
		2,	system,	exit,	exit_func
		3,	system,	hotfix,	hotfix_func
		n,	accept, method,	method_func
		m, 	response ...
	]]--
	local func = {}

	local system = { "init", "exit", "hotfix" }

	do
		for k, v in ipairs(system) do
			-- 遍历放入func库
			system[v] = k
			func[k] = { k , "system", v }
		end
	end

	env.accept = func_id(func, "accept")
	env.response = func_id(func, "response")

	local function init_system(t, name, f)
		local index = system[name]
		if index then
			if type(f) ~= "function" then
				error (string.format("%s must be a function", name))
			end
			func[index][4] = f
		else
			temp_global[name] = f
		end
	end

	local pattern

	do
		local path = assert(skynet.getenv "snax" , "please set snax in config file")

		local errlist = {}

		-- 查找snax文件
		for pat in string.gmatch(path,"[^;]+") do
			local filename = string.gsub(pat, "?", name)
			local f , err = loader(filename, "bt", G)
			if f then
				pattern = pat
				mainfunc = f
				break
			else
				table.insert(errlist, err)
			end
		end

		if mainfunc == nil then
			error(table.concat(errlist, "\n"))
		end
	end

	-- 设置环境变量元表，执行模块文件代码，清理环境变量元表
	setmetatable(G,	{ __index = env , __newindex = init_system })
	local ok, err = pcall(mainfunc)
	setmetatable(G, nil)
	assert(ok,err)

	-- 把其他的全局变量保存到环境变量中
	for k,v in pairs(temp_global) do
		G[k] = v
	end

	return func, pattern
end
