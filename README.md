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
  
## 3. 本地启动

  1. 到[这里](https://github.com/nsqio/nsq/releases/latest)下载二进制包并解压.
  
  2. 然后根据自己使用的操作系统将`start.sh`或者`start.cmd`复制到解压后的文件夹内.

  3. 打开`terminal`或`cmd`窗口, 执行上述脚本后即可实现本地快速启动.

## 注意

  1. 订阅的消息是无序的, 不保证消息的顺序与可靠性.

  2. 在内部尽可能使用TCP接口, 在外部请使用HTTP接口.
