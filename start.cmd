%后台运行`nsqlookupd`%
start .\nsqlookupd.exe > nsqlookupd.log 2>&1

%后台运行`nsqd`%
start .\nsqd --lookupd-tcp-address=127.0.0.1:4160 > nsqd.log 2>&1

%后台运行`nsqadmin`%
start .\nsqadmin --lookupd-http-address=127.0.0.1:4161 > nsqadmin.log 2>&1
