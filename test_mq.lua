require "utils"
local nsq = require "lua-nsq"

local mq = nsq:new{ domian = "localhost", port = 4150 }

mq:on("admin", "user", function (info)
  var_dump(info)
end)

mq:on("admin", "chat", function (info)
  var_dump(info)
end)

require "cf".at(1, function ()
  -- 发布多条消息
  mq:emit_all("admin", "Hello world. 1", "Hello world. 2")
end)

require "cf".timeout(10, function ()
  -- 发布多条消息
  mq:close()
end)