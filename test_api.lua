require "utils"
local hc = require "lua-nsq.httpc"
local httpc = hc:new{ domain = "localhost", port = 4151 }

-- 测试服务器状态
var_dump(assert(httpc:stats()))
-- 测试服务器版本
var_dump(assert(httpc:info()))

-- 创建主题
var_dump(assert(httpc:topic_create(nil, "admin")))
-- 创建通道
var_dump(assert(httpc:channel_create(nil, "admin", "user")))
var_dump(assert(httpc:channel_create(nil, "admin", "chat")))

-- 发布普通消息
var_dump(assert(httpc:pub(nil, "admin", "消息1", 0 or nil)))
-- 发布延迟消息
var_dump(assert(httpc:pub(nil, "admin", "消息2", 10)))

-- 清空消息
var_dump(assert(httpc:channel_empty(nil, "admin", "user")))
var_dump(assert(httpc:channel_empty(nil, "admin", "chat")))

-- 删除主题
var_dump(assert(httpc:topic_delete(nil, "admin")))