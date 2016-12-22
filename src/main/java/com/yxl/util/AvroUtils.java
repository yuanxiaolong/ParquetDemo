package com.yxl.util;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * avro 工具类
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public class AvroUtils {

    public static String toAvroSchema(String parquetSchemaFields){
        List<String> fields = Arrays.asList(StringUtils.split(StringUtils.trim(parquetSchemaFields),","));

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type","record");
        jsonObject.put("name","avro_schema");
        jsonObject.put("namespace","com.yxl");

        JSONArray array = new JSONArray();
        for (String field : fields){
            JSONObject object = new JSONObject();

            if (StringUtils.contains(field,":")){
                String[] arr = StringUtils.split(field,":");
                String name = arr[0];
                String type = arr[1];
                object.put("name",name);
                if (StringUtils.equalsIgnoreCase("long",type)){
                    object.put("type","[ \"$null$\",\"long\" ]");
                    object.put("default","null");
                }else if (StringUtils.equalsIgnoreCase("double",type)){
                    object.put("type","[ \"$null$\",\"double\" ]");
                    object.put("default","null");
                }else if (StringUtils.equalsIgnoreCase("boolean",type)){
                    object.put("type","[ \"$null$\",\"boolean\" ]");
                    object.put("default","null");
                }else if (StringUtils.equalsIgnoreCase("int",type)){
                    object.put("type","[ \"$null$\",\"int\" ]");
                    object.put("default","null");
                }else {
                    object.put("type","[ \"$null$\",\"string\" ]");
                    object.put("default","null");
                }
            }else{
                object.put("name",field);
                object.put("type","string");
                object.put("default","");
            }
            array.add(object);
        }

        jsonObject.put("fields", array);

        String s = jsonObject.toString();

        return StringUtils.replace(s, "$", "");//用$的目的是 json无法直接生成 "null" 这样的字符串
    }
}
