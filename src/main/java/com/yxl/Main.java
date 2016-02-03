package com.yxl;

import com.yxl.parquet.WriteParquet;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 入口函数
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            LOG.warn("Usage: " + " INPUTFILE OUTPUTFILE [compression gzip | snappy]");
            System.out.println("Usage: " + " INPUTFILE OUTPUTFILE [compression gzip | snappy]");
            return;
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String compression = (args.length > 2) ? args[2] : "none";
        try {
            ToolRunner.run(new WriteParquet(), new String[]{inputPath, outputPath, compression});
        } catch (Exception e) {
            LOG.error("run mr JOB convert parquet file happend error: ", e);
        }

    }

}
