local tcp = require "internal.TCP"
local sys = require "sys"

local LOG = require "logging"

local cf = require "cf"
local cf_fork = cf.fork

local json = require "json"
local json_encode = json.encode
local json_decode = json.decode

local ipairs = ipairs
local toint = math.tointeger
local fmt = string.format
local strpack = string.pack
local strunpack = string.unpack
local tconcat = table.concat
local tremove = table.remove

local MAGIC_V2 = '  V2'
local IDENTIFY = 'IDENTIFY\n'

-- 响应消息类型
local FRAME_TYPE_RESPONSE = 0
local FRAME_TYPE_ERROR    = 1
local FRAME_TYPE_MESSAGE  = 2

local function sock_read(sock, bytes)
	local buffer = sock:recv(bytes)
	if not buffer then
		return
	end
	if #buffer == bytes then
		return buffer
	end
	bytes = bytes - #buffer
	local buffers = {buffer}
  local sock_recv = sock.recv
	while 1 do
		buffer = sock_recv(sock, bytes)
		if not buffer then
			return
		end
    bytes = bytes - #buffer
		buffers[#buffers+1] = buffer
		if bytes == 0 then
			return tconcat(buffers)
		end
	end
end

local function sock_write(sock, data)
	return sock:send(data)
end

local function sock_connect(self)
  local sock = tcp:new()
  if not sock:connect(self.domain, self.port) then
    return
  end
  return sock
end

local function read_head(sock)
	local buffer = sock_read(sock, 8)
	if not buffer then
		return false, "[NSQ ERROR]: server closed."
	end
  local dsize, dtype = string.unpack(">I4I4", buffer)
  return { dsize = dsize, dtype = dtype}
end

local function read_response(sock)
  local opt, errinfo = read_head(sock)
  if not opt then
    return false, errinfo
  end
  -- 订阅消息
  if opt.dtype == FRAME_TYPE_MESSAGE then
    local buffer = sock_read(sock, 26)
    if not buffer then
      return false, "[NSQ ERROR]: server closed."
    end
    local body = sock_read(sock, opt.dsize - 30)
    if not body then
      return false, "[NSQ ERROR]: server closed."
    end
    local ts, _, mid = strunpack(">I8I2c16", buffer)
    opt.ok, opt.ts, opt.mid, opt.message = true, ts, mid, body
  -- 请求响应
  elseif opt.dtype == FRAME_TYPE_RESPONSE then
    local body = sock_read(sock, opt.dsize - 4)
    if not body then
      return false, "[NSQ ERROR]: server closed."
    end
    opt.ok = body == "OK" or body == "__heartbeat__"
  -- 错误响应
  elseif opt.dtype == FRAME_TYPE_ERROR then
    local body = sock_read(sock, opt.dsize - 4)
    if not body then
      return false, "[NSQ ERROR]: server closed."
    end
    opt.ok, opt.message = false, body
  end
  return opt
end

local function cmd_magic(sock)
  return sock_write(sock, MAGIC_V2)
end

local function cmd_identify(sock, no_heartbeat)
  return sock_write(sock, IDENTIFY) and sock_write(sock, strpack(">s4", json_encode{ feature_negotiation = true, user_agent = "lua_nsq/0.1", hostname = sys.hostname(), heartbeat_interval = no_heartbeat and -1 or nil }))
end

local function cmd_rdy(sock, num)
  return sock_write(sock, num and fmt("RDY %u\n", toint(num) or 200) or "RDY 200\n")
end

local function cmd_fin(sock, mid)
  return sock_write(sock, fmt("FIN %s\n", mid))
end

local function cmd_subscribe(sock, topic, channel)
  return sock_write(sock, fmt("SUB %s %s\n", topic, channel))
end

-- 延迟发布消息
local function cmd_dpublish(sock, topic, message, dtime)
  return sock_write(sock, fmt("DPUB %s %u\n", topic, dtime)) and sock_write(sock, strpack(">s4", message))
end

-- 批量发布消息
local function cmd_mpublish(sock, topic, ...)
  local bsize = 0
  local info = {}
  local array = {...}
  for index, msg in ipairs(array) do
    local message = strpack(">s4", msg)
    bsize = bsize + #message
    info[index + 1] = message
  end
  info[1] = strpack(">I4I4", bsize + 8, #array)
  return sock_write(sock, fmt("MPUB %s\n", topic)) and sock_write(sock, tconcat(info))
end

local function cmd_nop(sock)
  return sock_write(sock, "NOP\n")
end

local function cmd_handshake(self, no_heartbeat)
  -- 连接服务器
  local sock = sock_connect(self)
  if not sock then
    return false, "[NSQ ERROR]: Can't Connect to nsq server 1."
  end
  -- 发送魔发字符
  if not cmd_magic(sock) or not cmd_identify(sock, no_heartbeat) then
    sock:close()
    return false, "[NSQ ERROR]: Can't Connect to nsq server 2."
  end
  -- 解析响应
  local opt, errinfo = read_response(sock)
  if not opt then
    sock:close()
    return false, errinfo or opt.message
  end
  -- 验证
  opt = json_decode(opt.message) or {}
  if opt.auth_required then
    sock:close()
    return false, "[NSQ ERROR]: Can't auth this session."
  end
  self.opt = opt
  return sock
end

local protocol = {}

-- 投递延迟消息
function protocol.dpublish(self, topic, message, dtime)
  if not self.pool then
    self.pool = {}
  end
  local sock = tremove(self.pool)
  if not sock then
    sock = cmd_handshake(self, true)
  end
  local info = cmd_dpublish(sock, topic, message, toint(dtime) or 0)
  if not info then
    for _, s in ipairs(self.pool) do
      s:close()
    end
    self.pool = nil
  else
    if self.closed then
      sock:close()
    else
      self.pool[#self.pool+1] = sock
    end
  end
  return info
end

-- 批量投递消息
function protocol.mpublish(self, topic, message, ...)
  if not self.pool then
    self.pool = {}
  end
  local sock = tremove(self.pool)
  if not sock then
    sock = cmd_handshake(self, true)
  end
  local info = cmd_mpublish(sock, topic, message, ...)
  if not info then
    for _, s in ipairs(self.pool) do
      s:close()
    end
    self.pool = nil
  else
    if self.closed then
      sock:close()
    else
      self.pool[#self.pool+1] = sock
    end
  end
  return info
end

-- 订阅消息
function protocol.subscribe(self, topic, channel, callback)
  local sock = assert(cmd_handshake(self))
  assert(cmd_subscribe(sock, topic, channel) and cmd_rdy(sock), "[NSQ ERROR]: Can't subscribe any topic or channel.")
  cf_fork(function ()
    local count = 200
    local key = topic .. '=' .. channel
    self.map[key] = sock
    while true do
      local info = read_response(sock)
      if info then
        -- var_dump(info)
        -- 消息类型
        local dtype = info.dtype
        if dtype == FRAME_TYPE_MESSAGE then
          cf_fork(callback, { message = info.message, mid = info.mid, ts = info.ts * 1e-9 })
          cmd_fin(sock, info.mid) -- 主动响应`fin`
          count = count - 1
          if count == 50 then
            count = 200
            cmd_rdy(sock)
          end
        elseif dtype == FRAME_TYPE_RESPONSE then
          cmd_nop(sock) -- 收到任何`RESPONSE`类型响应都回应`NOP`.
        else
          LOG:ERROR(info)
        end
      end
      -- 需要实现自会恢复的机制, 这样能保障内部的可用性.
      if not info then
        if self.closed then
          return
        end
        while true do
          if sock then
            sock:close()
          end
          sock = cmd_handshake(self)
          if sock and cmd_subscribe(sock, topic, channel) and cmd_rdy(sock) then
            break
          end
        end
        if self.closed then
          return sock:close()
        end
        self.map[key] = sock
        count = 200
      end
    end
  end)
end

return protocol
