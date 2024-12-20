# Import

### Base

```
Import completed in 3m27.1020196s
Load popularity: 877.9945ms
Load taxonomy: 19.4745013s
Merge vertices: 1.3815034s
Insert vertices: 39.9248621s
Fetch vertex RIDs: 44.1891578s
Insert edges: 1m41.2535026s
```

### Disabled transactions

```
Import completed in 4m35.3089838s
Load popularity: 864.5032ms
Load taxonomy: 16.0279984s
Merge vertices: 1.209497s
Insert vertices: 1m6.3893077s
Fetch vertex RIDs: 23.8114976s
Insert edges: 2m47.0061799s
```

### Flags

```
-XX:+PerfDisableSharedMem -Dstorage.wal.syncOnPageFlush=false -Dtx.useLog=false
```

```
Import completed in 2m54.078587s
Load popularity: 910.4966ms
Load taxonomy: 15.9630007s
Merge vertices: 1.250504s
Insert vertices: 37.6747767s
Fetch vertex RIDs: 25.3485639s
Insert edges: 1m32.9312451s
```

### Flags + server config V1

```
Import completed in 3m36.8632487s
Load popularity: 931.9997ms
Load taxonomy: 16.3659995s
Merge vertices: 1.2109997s
Insert vertices: 41.5047494s
Fetch vertex RIDs: 26.2349996s
Insert edges: 2m10.6145008s
```

### Flags + server config V2

```
Import completed in 3m11.616663s
Load popularity: 834.9979ms
Load taxonomy: 17.264501s
Merge vertices: 1.5715016s
Insert vertices: 42.9805192s
Fetch vertex RIDs: 24.5324975s
Insert edges: 1m44.4326458s
```