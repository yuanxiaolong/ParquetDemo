package com.yxl.util;

import com.yxl.common.LaunchDO;
import org.apache.commons.lang.StringUtils;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 转换 parquet schema 工具
 * Created by xiaolong.yuanxl on 16-12-14.
 */
public class SchemaUtils {

    public static String generateStringSchema(String input){
        List<String> lines = Arrays.asList(StringUtils.split(input,","));
        return generateStringSchema(lines);
    }

    public static String generateStringSchema(List<String> lines){
        StringBuffer sb = new StringBuffer();
        sb.append("message hive_schema {\n");
        for (String line : lines){
            sb.append("\toptional binary "+ StringUtils.trim(line) +" (UTF8);\n");
        }
        sb.append("}");
        return sb.toString();
    }

    public static MessageType generateSchema(String input){
        return  MessageTypeParser.parseMessageType(generateStringSchema(input));
    }

}
