# concurrent_tcpstore

Performance numbers:


At 100 workers with (1 set, 1 wait, 1 get):
```
baseline:
TCPStore: 339*3*100 = 101k qps

custom:
ChannelStore: (~650-750) 700*3*100 = 210k qps
LockStore: (731-826) 780*3*100 = 234k qps
ConcurrentStore: ~770*3*100 = 231k qps
```

Notes:

* locking channelstore thread is actually slower
