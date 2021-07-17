local protocol = require "lua-nsq.protocol"
local protocol_dpublish = protocol.dpublish
local protocol_mpublish = protocol.mpublish
local protocol_subscribe = protocol.subscribe

local type = type
local assert = assert

local class = require "class"

local nsq = class("nsq")

function nsq:ctor(opt)
  assert(type(opt) == 'table', "[NSQ ERROR]: Invalid opt.")
  self.domain = opt.domain or "localhost"
  self.port = opt.port or 4150
  self.map = {}
end

---comment 订阅消息
---@param topic     string    @订阅的主题
---@param channel   string    @订阅的通道
---@param callback  function  @收到订阅消息会触发次回调函数
function nsq:on(topic, channel, callback)
  assert(type(topic) == 'string', "[NSQ ERROR]: `topic` must be string type.")
  assert(type(channel) == 'string', "[NSQ ERROR]: `channel` must be string type.")
  assert(type(callback) == 'function', "[NSQ ERROR]: `callback` must be function type.")
  return protocol_subscribe(self, topic, channel, callback)
end

---comment 发布消息
---@param topic     string    @主题名称
---@param message   string    @消息内容(必须)
---@param dtime     number    @延迟时间(可选)
function nsq:emit_one(topic, message, dtime)
  assert(not self.closed, "[NSQ ERROR]: already closed.")
  assert(type(topic) == 'string', "[NSQ ERROR]: `channel` must be string type.")
  assert(type(dtime) == 'number', "[NSQ ERROR]: `dtime` must be number type.")
  assert(type(message) == 'string', "[NSQ ERROR]: `message` must be string type.")
  return protocol_dpublish(self, topic, message, dtime)
end

---comment 批量发布消息
---@param topic     string    @发布的主题
---@param message   string    @发布的消息(`...`表示可以一次发布多个`message`)
function nsq:emit_all(topic, message, ...)
  assert(not self.closed, "[NSQ ERROR]: already closed.")
  assert(type(topic) == 'string', "[NSQ ERROR]: `channel` must be string type.")
  assert(type(message) == 'string', "[NSQ ERROR]: `message` must be string type.")
  return protocol_mpublish(self, topic, message, ...)
end

function nsq:close()
  self.closed = true
  -- 关闭发布消息的连接池
  if self.pool then
    for _, sock in pairs(self.pool) do
      sock:close()
    end
    self.pool = nil
  end
  -- 关闭订阅消息的连接池
  if self.map then
    for _, sock in pairs(self.map) do
      sock:close()
    end
    self.map = nil
  end
end

return nsq