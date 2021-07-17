# lua-nsq

  基于`cfadmin`实现的`NSQ`客户端驱动.

## 优势 

  - [x] 完全的`lua`实现, 代码清晰易懂

  - [x] 完善的使用注释, 上手非常轻松

  - [x] 协程与连接池支持, 使用非常方便
  

## 构建

  将本项目克隆到`3rd`目录即可开始使用.

## 示例

## 1. HTTP 使用

  参考[这里](https://github.com/CandyMi/lua-nsq/blob/master/test_api.lua)的示例代码.

## 2. TCP 使用

  参考[这里](https://github.com/CandyMi/lua-nsq/blob/master/test_mq.lua)的示例代码.

## 注意

  1. 订阅的消息是无序的, 不保证消息的顺序与可靠性.

  2. 在内部尽可能使用TCP接口, 在外部请使用HTTP接口.