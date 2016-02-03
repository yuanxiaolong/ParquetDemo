package com.yxl.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

/**
 * 写文件为parquet格式
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class WriteParquet extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(WriteParquet.class);

    @Override
    public int run(String[] strings) throws Exception {
        String input = strings[0];
        String output = strings[1];
        String compression = strings[2];

        Configuration conf = new Configuration();

        // 删除已有结果集
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(output);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }


        Job job = Job.getInstance();

        job.setJobName("Convert Text to Parquet");
        job.setJarByClass(getClass());

        job.setMapperClass(WriteParquetMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);
        ExampleOutputFormat.setSchema(job, WriteParquetMapper.SCHEMA);
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
