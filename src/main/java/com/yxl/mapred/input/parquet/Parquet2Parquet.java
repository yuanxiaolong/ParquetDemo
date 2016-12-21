package com.yxl.mapred.input.parquet;

import com.yxl.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;

/**
 * Parquet 输入 处理后输出
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public class Parquet2Parquet extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(Parquet2Parquet.class);

    @Override
    public int run(String[] strings) throws Exception {

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);
        String schema = strings[2];
        String compression = strings[3];
        boolean isOverwrite = Boolean.valueOf(strings[4]);

        Job job = Job.getInstance(getConf(), "AvroParquetMapReduce");
        job.setJarByClass(getClass());

        // 删除已有结果集
        if (isOverwrite){
            FileSystem fs = FileSystem.get(getConf());
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
        }

        Schema avroSchema = new Schema.Parser().parse(AvroUtils.toAvroSchema(schema));

        getConf().set("schema",schema);//向mapper传递

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setInputPaths(job, inputPath);

        job.setMapperClass(Parquet2ParquetMapper.class);
        job.setNumReduceTasks(0);


        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);


        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);

        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        if(compression.equalsIgnoreCase("snappy")) {
            codec = CompressionCodecName.SNAPPY;
        } else if(compression.equalsIgnoreCase("gzip")) {
            codec = CompressionCodecName.GZIP;
        }
        LOG.info("Output compression: " + codec);
        AvroParquetOutputFormat.setCompression(job, codec);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

}
