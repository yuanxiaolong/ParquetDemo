package com.yxl.parquet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * 写parquet mapper
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class WriteParquetMapper extends Mapper<LongWritable, Text, Void, Group> {

    public static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
//            "message Line {\n" +
//                    "  required int64 offset;\n" +
//                    "  required binary line (UTF8);\n" +
//                    "}"
            "message hive_schema {\n" +
                    "  optional binary remote_addr (UTF8);\n" +
                    "  optional binary upstream_addr (UTF8);\n" +
                    "  optional binary http_x_forwarded_for (UTF8);\n" +
                    "  optional binary visit_time (UTF8);\n" +
                    "  optional binary request_uri (UTF8);\n" +
                    "  optional binary request_method (UTF8);\n" +
                    "  optional binary server_protocol (UTF8);\n" +
                    "  optional binary status (UTF8);\n" +
                    "  optional binary body_bytes_sent (UTF8);\n" +
                    "  optional binary request_time (UTF8);\n" +
                    "  optional binary uid (UTF8);\n" +
                    "  optional binary uuid (UTF8);\n" +
                    "  optional binary user_agent (UTF8);\n" +
                    "  optional binary refer (UTF8);\n" +
                    "  optional binary request_body (UTF8);\n" +
                    "}"
    );

    private GroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = StringUtils.trim(value.toString());
        String[] arr = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
        Group group = groupFactory.newGroup();
        try{
            if (arr != null){
                //直接获取下标
                group
                    .append("remote_addr", arr[0])
                    .append("upstream_addr", arr[1])
                    .append("http_x_forwarded_for", arr[2])
                    .append("visit_time", arr[3])
                    .append("request_uri",arr[4])
                    .append("request_method",arr[5])
                    .append("server_protocol", arr[6])
                    .append("status",arr[7])
                    .append("body_bytes_sent",arr[8])
                    .append("request_time", arr[9])
                    .append("uid", arr[10])
                    .append("uuid", arr[11])
                    .append("user_agent", arr[12])
                    .append("refer", arr[13])
                    .append("request_body", arr[14]);

            }
        }catch (Exception e){
            System.out.println("[ERROR]: map happend error " + e.getMessage());
        }
//                .append("offset", key.get())
//                .append("line", value.toString());
        context.write(null, group);
    }

}
