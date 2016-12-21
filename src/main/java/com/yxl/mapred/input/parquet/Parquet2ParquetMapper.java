package com.yxl.mapred.input.parquet;

import com.yxl.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parquet mapper 业务逻辑类
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public class Parquet2ParquetMapper extends Mapper<Void, GenericRecord, Void, GenericRecord> {

    private List<String> fields = new ArrayList<String>();

    private Schema s = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String schema = context.getConfiguration().get("schema");
        fields = Arrays.asList(StringUtils.split(StringUtils.trim(schema), ","));
        s = new Schema.Parser().parse(AvroUtils.toAvroSchema(schema));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        GenericRecordBuilder b = new GenericRecordBuilder(s);
        for (String k : fields)
            if (StringUtils.contains(k,":")){
                String[] arr = StringUtils.split(k,":");
                String name = arr[0];
                String type = arr[1];

                if (StringUtils.equalsIgnoreCase("long",type)){
                    b.set(name, Long.valueOf(value.get(name).toString()));
                }else if (StringUtils.equalsIgnoreCase("double",type)){
                    b.set(name, Double.valueOf(value.get(name).toString()));
                }else if (StringUtils.equalsIgnoreCase("boolean",type)){
                    b.set(name, Boolean.valueOf(value.get(name).toString()));
                }else if (StringUtils.equalsIgnoreCase("int",type)){
                    b.set(name, Integer.valueOf(value.get(name).toString()));
                }else {
                    ByteBuffer bf = (ByteBuffer) value.get(name);
                    b.set(name,new String(bf.array(),"UTF8"));
                }
            }else{
                ByteBuffer bf = (ByteBuffer) value.get(k);
                b.set(k,new String(bf.array(),"UTF8"));
            }
        GenericRecord output = b.build();

        //这里就是自定义的业务逻辑了
        String event_code = (String)output.get("event_code");
        if ("page".equalsIgnoreCase(event_code)){
            context.write(null, output);
        }
    }

}
