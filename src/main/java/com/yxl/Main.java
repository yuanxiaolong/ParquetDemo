package com.yxl;

import com.yxl.common.LaunchDO;
import com.yxl.parquet.WriteParquet;
import com.yxl.util.XmlUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 入口函数
 *
 * Created by xiaolong.yuanxl on 16-1-28.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            LOG.warn("Usage: Please input configuration xml file local path");
            System.out.println("Usage: Please input configuration xml file local path");
            return;
        }

        try {
            LaunchDO launchDO = XmlUtils.toLaunchProp(args[0]);
            ToolRunner.run(new WriteParquet(), new String[]{launchDO.getInput(), launchDO.getOutput(),
                    launchDO.getSchema(), launchDO.getSep(), launchDO.getCompress()});
        } catch (Exception e) {
            LOG.error("run mr JOB convert parquet file happend error: ", e);
        }

    }

}
