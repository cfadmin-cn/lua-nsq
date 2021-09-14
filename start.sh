#!/usr/bin/env bash

# 后台运行`nsqlookupd`
./nsqlookupd > nsqlookupd.log 2>&1 &

# 后台运行`nsqd`
./nsqd --lookupd-tcp-address=127.0.0.1:4160 > nsqd.log 2>&1 &

# 后台运行`nsqadmin`
./nsqadmin --lookupd-http-address=127.0.0.1:4161 > nsqadmin.log 2>&1 &