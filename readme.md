## 介绍

此工程用于通用转换 textfile或parquetfile 为 parquetfile 主要是应用于 提高hive表的查询效率或ETL处理


## 准备

需要准备一个 xml 配置文件,用于描述 生成parquet文件的schema 及 Input Output hdfs 路径等

示例如下:

```
<launch>
    <input>
        /test/xiaolong_1
    </input>
    <output>
        /user/hive/warehouse/test.db/tb_normal_sep
    </output>
    <schema>
            remote_addr,
            upstream_addr,
            http_x_forwarded_for,
            visit_time,
            request_uri,
            request_method,
            server_protocol,
            status,
            body_bytes_sent,
            request_time,
            uid,
            uuid,
            user_agent,
            refer,
            request_body
    </schema>
    <sep>
        \t
    </sep>
    <compress>
        snappy
    </compress>
    <isOverwrite>true</isOverwrite>
</launch>
```

说明:

* input 输入的hdfs路径
* output 输出的hdfs路径,如果不为空,则会先清空此目录
* schema 生成的parquet文件元信息
* sep 输入hdfs文件的分隔符
* compress 压缩形式,可选有 snappy 或 gzip 两种
* isOverwrite 是否覆盖写入,如果为true会先删除output再执行MR,请仔细.


## 遗留问题

目前只支持textfile源文件为 ```\t``` 分隔

## 改进

