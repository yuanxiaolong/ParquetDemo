package com.yxl.mapred.input.text;

import com.yxl.util.SchemaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * 写文件为parquet格式
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class Text2Parquet extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(Text2Parquet.class);

    @Override
    public int run(String[] strings) throws Exception {
        String input = strings[0];
        String output = strings[1];
        String schema = strings[2];
        String sep = strings[3];
        String compression = strings[4];

        Configuration conf = new Configuration();

        // 删除已有结果集
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(output);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        //1.初始化一个默认的conf,并将外部值传递到conf里,便于在mapper的 setup 方法里调用
        Job job = Job.getInstance();
        Configuration jobConf = job.getConfiguration();
        jobConf.set("schema",schema);
        jobConf.set("sep",sep);

        job.setJobName("Convert Text to Parquet");
        job.setJarByClass(getClass());
        //2.设置mapper
        job.setMapperClass(Text2ParquetMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);
        ExampleOutputFormat.setSchema(job, SchemaUtils.generateSchema(schema));
        job.setNumReduceTasks(0);   //不需要reduce
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        //设置压缩
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        if (compression.equalsIgnoreCase("snappy")) {
            codec = CompressionCodecName.SNAPPY;
        } else if (compression.equalsIgnoreCase("gzip")) {
            codec = CompressionCodecName.GZIP;
        }
        LOG.info("Output compression: " + codec);
        ExampleOutputFormat.setCompression(job, codec);


        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
