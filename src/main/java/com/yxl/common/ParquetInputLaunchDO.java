package com.yxl.common;

/**
 * Parquet文件输入lanuch
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public class ParquetInputLaunchDO extends InputLanuchDO{

    private String schema;


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
