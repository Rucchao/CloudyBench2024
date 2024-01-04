# CloudBench - A New Benchmark for Cloud-Native Databases 

## TODO (2024.1.3)
* 多租户发压 (完成双租户发压接口)
```
./hybench -t runCloudTP -c conf/db.prop -f conf/stmt_mysql.toml -m 2
```
* IOPS: 多租户同时事务请求 (完成runcloudTP接口实现)
* 渐进式发压
