local httpc_get = require "httpc".get
local httpc_post = require "httpc".post

local json = require "json"
local json_decode = json.decode

local type = type
local assert = assert
local fmt = string.format
local toint = math.tointeger

local function wrap_url(url, path)
  return url .. path
end

local class = require "class"

local httpc = class("nsq-httpc")

function httpc:ctor(opt)
  assert(type(opt) == 'table', "[NSQ ERROR]: Invalid new opt.")
  self.schema = opt.schema or "http"
  self.domain = assert(type(opt.domain) == 'string' and opt.domain ~= '' and opt.domain, "[NSQ ERROR]: Invalid `domain`.")
  self.port = assert(type(opt.port) == 'number' and opt.port >= 80 and opt.port, "[NSQ ERROR]: Invalid `port`.")
  self.url = fmt("%s://%s:%s", self.schema, self.domain, self.port)
end

---comment 获取服务器状态
---@param headers table @头部键值对
---@param args table    @参数键值对
function httpc:stats(headers, args)
  local code, response = httpc_get(wrap_url(self.url, "/stats?format=json"), headers, args)
  if code ~= 200 then
    return false, "[NSQ ERROR]: " .. (response or "Invalide response.")
  end
  return json_decode(response)
end

---comment 获取服务器版本信息
---@param headers table @头部键值对
---@param args table    @参数键值对
function httpc:info(headers, args)
  local code, response = httpc_get(wrap_url(self.url, "/info?format=json"), headers, args)
  if code ~= 200 then
    return false, "[NSQ ERROR]: " .. (response or "Invalide response.")
  end
  return json_decode(response)
end

---comment 创建主题
---@param headers table @头部键值对
---@param topic  string @主题名称
function httpc:topic_create(headers, topic)
  local code = httpc_post(wrap_url(self.url, "/topic/create?topic=" .. topic), headers, "")
  if code ~= 200 then
    return false, "[NSQ ERROR]: Invalide response."
  end
  return true
end

---comment 删除主题
---@param headers table @头部键值对
---@param topic  string @主题名称
function httpc:topic_delete(headers, topic)
  local code = httpc_post(wrap_url(self.url, "/topic/delete?topic=" .. topic), headers, "")
  if not code then
    return false, "[NSQ ERROR]: Invalide response."
  end
  if code == 404 then
    return false, "[NSQ ERROR]: Can't find this topic."
  end
  return true
end

---comment 清空主题消息
---@param headers table @头部键值对
---@param topic  string @主题名称
function httpc:topic_empty(headers, topic)
  local code = httpc_post(wrap_url(self.url, "/topic/empty?topic=" .. topic), headers, "")
  if not code then
    return false, "[NSQ ERROR]: Invalide response."
  end
  if code == 404 then
    return false, "[NSQ ERROR]: Can't find this topic."
  end
  return true
end

---comment 创建通道
---@param headers  table  @头部键值对
---@param topic    string @主题名称
---@param channel  string @通道名称
function httpc:channel_create(headers, topic, channel)
  local code = httpc_post(wrap_url(self.url, "/channel/create?topic=" .. topic .. "&channel=" .. channel), headers, "")
  if code ~= 200 then
    return false, "[NSQ ERROR]: Invalide response."
  end
  return true
end

---comment 删除通道
---@param headers table @头部键值对
---@param topic  string @主题名称
---@param channel  string @通道名称
function httpc:channel_delete(headers, topic, channel)
  local code = httpc_post(wrap_url(self.url, "/channel/delete?topic=" .. topic .. "&channel=" .. channel), headers, "")
  if not code then
    return false, "[NSQ ERROR]: Invalide response."
  end
  if code == 404 then
    return false, "[NSQ ERROR]: Can't find this channel."
  end
  return true
end

---comment 清空通道消息
---@param headers table @头部键值对
---@param topic  string @主题名称
function httpc:channel_empty(headers, topic, channel)
  local code = httpc_post(wrap_url(self.url, "/channel/empty?topic=" .. topic .. "&channel=" .. channel), headers, "")
  if not code then
    return false, "[NSQ ERROR]: Invalide response."
  end
  if code == 404 then
    return false, "[NSQ ERROR]: Can't find this channel."
  end
  return true
end

---comment 投递消息
---@param headers table @头部键值对
---@param topic  string @主题名称(必填)
---@param body   string @消息内容(必填)
---@param dtime  string @延迟时间(可选)
function httpc:pub(headers, topic, body, dtime)
  local code, response = httpc_post(wrap_url(self.url, "/pub?topic=" .. topic .. "&defer=" .. (toint(dtime) or 0)), headers, body)
  if not code then
    return false, "[NSQ ERROR]: Invalide response."
  end
  return response == "OK"
end

return httpc