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

ChannelStore (1 proc): 348
ChannelStore (2 proc): 606
ChannelStore (4 proc): 1031
ChannelStore (8 proc): 1760
ChannelStore (10 procs): 2275 * 3 * 100 = 682k qps
ChannelStore (16 procs, 100 workers): 2141 * 3 * 100
ChannelStore (16 procs, 200 workers): 1130 * 3 * 200
ChannelStore (16 procs, 1000 workers): 200 * 3 * 1000

LockStore (10 procs, 100 workers): 390k qps

ConcurrentStore (10 procs, 100 workers, 100 keys) = 850k qps
ConcurrentStore (20 procs, 100 workers, 100 keys) = 1450k qps
ConcurrentStore (40 procs, 100 workers, 100 keys) = 1207k qps
ConcurrentStore (100 procs, 100 workers, 100 keys) = 331k qps


ConcurrentStore (10 procs, 100 clients, 1 key) = 967k qps
ConcurrentStore (16 procs, 100 clients, 1 key) = 1200k qps
ConcurrentStore (20 procs, 100 clients, 1 key) = 1163k qps
ConcurrentStore (30 procs, 100 clients, 1 key) = 1000k qps

ConcurrentStore (16 procs, 1000 clients, 1 key) = 800k qps
ConcurrentStore (30 procs, 1000 clients, 1 key) = 850k qps
ConcurrentStore (16 procs, 2000 clients, 1 key) = 600k qps

ChannelStore (16 procs, 2000 clients, 1 key) = 550k qps

```

Notes:

* locking channelstore thread is actually slower
