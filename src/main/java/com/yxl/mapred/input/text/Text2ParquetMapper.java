package com.yxl.mapred.input.text;

import com.yxl.util.SchemaUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 写parquet mapper
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class Text2ParquetMapper extends Mapper<LongWritable, Text, Void, Group> {

//    public static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
////            "message Line {\n" +
////                    "  required int64 offset;\n" +
////                    "  required binary line (UTF8);\n" +
////                    "}"
//            "message hive_schema {\n" +
//                    "  optional binary remote_addr (UTF8);\n" +
//                    "  optional binary upstream_addr (UTF8);\n" +
//                    "  optional binary http_x_forwarded_for (UTF8);\n" +
//                    "  optional binary visit_time (UTF8);\n" +
//                    "  optional binary request_uri (UTF8);\n" +
//                    "  optional binary request_method (UTF8);\n" +
//                    "  optional binary server_protocol (UTF8);\n" +
//                    "  optional binary status (UTF8);\n" +
//                    "  optional binary body_bytes_sent (UTF8);\n" +
//                    "  optional binary request_time (UTF8);\n" +
//                    "  optional binary uid (UTF8);\n" +
//                    "  optional binary uuid (UTF8);\n" +
//                    "  optional binary user_agent (UTF8);\n" +
//                    "  optional binary refer (UTF8);\n" +
//                    "  optional binary request_body (UTF8);\n" +
//                    "}"
//    );

    private MessageType SCHEMA;

    private String sep = "\t";

    private List<String> keys = new ArrayList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String str = context.getConfiguration().get("schema");
        sep = context.getConfiguration().get("sep");
//        System.out.println("[SETUP] sep: " + sep);
        keys = Arrays.asList(StringUtils.split(str,","));
        SCHEMA = SchemaUtils.generateSchema(str);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = StringUtils.trim(value.toString());
//        System.out.println("[USE] sep: " + sep);
        //FIXME TODO    this is very strange , replace hard code \t to variable sep ,is not working ,read whole line without split
        //FIXME TODO    appreciate someone who want pull request fix this problem
        String[] arr = StringUtils.splitByWholeSeparatorPreserveAllTokens(line,"\t");
        Group group = new SimpleGroupFactory(SCHEMA).newGroup();
        try{
            if (arr != null){
                //直接获取下标
                for (int i = 0; i < arr.length; i++) {
                    group.append(keys.get(i),arr[i]);
                }
            }
        }catch (Exception e){
            System.out.println("[ERROR]: map happend error " + e.getMessage());
        }
//                .append("offset", key.get())
//                .append("line", value.toString());
        context.write(null, group);
    }

}
