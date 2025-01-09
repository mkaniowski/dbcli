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

### New load csv + flags + config v2 (there is more loaded data)

```
Import completed in 3m9.9764163s
Load popularity: 977.9999ms
Load taxonomy: 3.4749995s
Merge vertices: 1.476001s
Fetch vertex RIDs: 26.2415091s
Insert edges: 1m45.1485462s
```

### Before + 15k batch size + 4 workers on insert vertecies

```
Import completed in 2m48.9849358s
Load popularity: 1.0000025s
Load taxonomy: 3.6504951s
Merge vertices: 1.3995009s
Insert vertices: 25.6110031s
Fetch vertex RIDs: 32.3749376s
Insert edges: 1m44.9489966s
```

### Before + 6 workers on insert vertecies + 6 workers on insert edges

```
Import completed in 2m3.3174935s
Load popularity: 1.0834987s
Load taxonomy: 4.2345028s
Merge vertices: 1.8449972s
Insert vertices: 27.9730044s
Fetch vertex RIDs: 28.8044898s
Insert edges: 59.3770006s
```

### Before + fixed config v2

```
Import completed in 1m47.453s
Load popularity: 918.5ms
Load taxonomy: 3.8750014s
Merge vertices: 1.455499s
Insert vertices: 24.3284504s
Fetch vertex RIDs: 25.8520484s
Insert edges: 51.0230013s
```

### Before + docker changes

```
Import completed in 1m33.1430328s
Load popularity: 1.1099872s
Load taxonomy: 5.8295469s
Merge vertices: 1.2894973s
Insert vertices: 21.8497755s
Fetch vertex RIDs: 17.5777254s
Insert edges: 45.4725002s
```