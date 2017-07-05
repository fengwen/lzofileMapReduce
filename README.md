lzofileMapReduce lzo文件通过Map方式读取格式化

# 功能描述
清洗lzo格式文件将数据清洗到指定目录，包括文件校验、清洗分割符等。
1. 支持map计数
2. 支持lzo结尾的校验文件读取,通过校验文件实现数据校验
3. 支持全量、增量文件列表处理
4. 支持执行map需要处理文件列表
5. 支持指定输出目录文件名


# 执行
```
mvn clean package
hadoop jar etl4lzofile-0.0.1-SNAPSHOT.jar /data/source/message/ /data/source/message-done/SPINFO SPINFO lzo add
```

