package com.yxl;

import com.yxl.common.ParquetInputLaunchDO;
import com.yxl.common.TextInputLaunchDO;
import com.yxl.mapred.input.parquet.Parquet2Parquet;
import com.yxl.mapred.input.text.Text2Parquet;
import com.yxl.util.XmlUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 主入口函数
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            LOG.warn("Usage: Please enter [input_file_type] [configuration xml file local path]");
            System.out.println("Usage: Please enter [input_file_type] [configuration xml file local path]");
            return;
        }

        try {
            String fileType = args[0];
            String filePath = args[1];
            if (StringUtils.equalsIgnoreCase("parquet",fileType)){
                ParquetInputLaunchDO launchDO = XmlUtils.toParquetLaunchProp(filePath);
                ToolRunner.run(new Parquet2Parquet(), new String[]{launchDO.getInput(), launchDO.getOutput(),
                        launchDO.getSchema(), launchDO.getCompress(), String.valueOf(launchDO.isOverwrite())});
            }else if (StringUtils.equalsIgnoreCase("text",fileType)){
                TextInputLaunchDO launchDO = XmlUtils.toTextLaunchProp(filePath);
                ToolRunner.run(new Text2Parquet(), new String[]{launchDO.getInput(), launchDO.getOutput(),
                        launchDO.getSchema(), launchDO.getSep(), launchDO.getCompress(),String.valueOf(launchDO.isOverwrite())});
            }else{
                System.out.println("unknow input file type: " + fileType);
            }
        } catch (Exception e) {
            LOG.error("run mr JOB convert parquet file happend error: ", e);
        }
    }

}
