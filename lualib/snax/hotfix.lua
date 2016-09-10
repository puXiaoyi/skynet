local si = require "snax.interface"

-- 返回f的_ENV上值
local function envid(f)
	local i = 1
	while true do
		-- 返回函数 f 的第 i 个上值的名字和值。 如果该函数没有那个上值，返回 nil
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			return
		end
		if name == "_ENV" then
			-- 返回函数 f 第 i 个上值的唯一标识符（一个轻量用户数据）
			return debug.upvalueid(f, i)
		end
		i = i + 1
	end
end

-- 收集f所有的上值
local function collect_uv(f , uv, env)
	local i = 1
	-- 遍历f所有上值
	while true do
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			-- 没有上值，中止遍历
			break
		end
		local id = debug.upvalueid(f, i)

		if uv[name] then
			-- 不允许两个相同名字的上值
			assert(uv[name].id == id, string.format("ambiguity local value %s", name))
		else
			uv[name] = { func = f, index = i, id = id }

			-- 如果上值为function且_ENV相同，继续迭代收集上值
			if type(value) == "function" then
				if envid(value) == env then
					collect_uv(value, uv, env)
				end
			end
		end

		i = i + 1
	end
end

-- 收集funcs所有的上值
local function collect_all_uv(funcs)
	local global = {}
	-- 遍历所有snax方法
	for _, v in pairs(funcs) do
		if v[4] then
			collect_uv(v[4], global, envid(v[4]))
		end
	end
	if not global["_ENV"] then
		global["_ENV"] = {func = collect_uv, index = 1}
	end
	return global
end

-- 加载代码块
local function loader(source)
	return function (filename, ...)
		-- 加载hotfix的patch代码块
		return load(source, "=patch", ...)
	end
end

-- 查找group和name对应的func
local function find_func(funcs, group , name)
	for _, desc in pairs(funcs) do
		local _, g, n = table.unpack(desc)
		if group == g and name == n then
			return desc
		end
	end
end

local dummy_env = {}

-- 打补丁
local function patch_func(funcs, global, group, name, f)
	-- 判断补丁中方法是否存在于原snax服务中，不能新增accept/response方法
	local desc = assert(find_func(funcs, group, name) , string.format("Patch mismatch %s.%s", group, name))
	local i = 1
	-- 关联上值
	while true do
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			-- 没有上值，中止遍历
			break
		elseif value == nil or value == dummy_env then
			-- 
			local old_uv = global[name]
			if old_uv then
				-- 让 Lua 闭包 f 的第 i 个上值 引用 Lua 闭包 old_uv.func 的第 old_uv.index 个上值
				debug.upvaluejoin(f, i, old_uv.func, old_uv.index)
			end
		end
		i = i + 1
	end
	-- 替换方法
	desc[4] = f
end

-- funcs, snax.interface 
-- source, hotfix patch chunk
-- ..., parameter for hotfix function in patch chunk
local function inject(funcs, source, ...)
	local patch = si("patch", dummy_env, loader(source))
	local global = collect_all_uv(funcs)

	for _, v in pairs(patch) do
		local _, group, name, f = table.unpack(v)
		if f then
			patch_func(funcs, global, group, name, f)
		end
	end

	-- 执行hotfix方法
	local hf = find_func(patch, "system", "hotfix")
	if hf and hf[4] then
		return hf[4](...)
	end
end

return function (funcs, source, ...)
	return pcall(inject, funcs, source, ...)
end
