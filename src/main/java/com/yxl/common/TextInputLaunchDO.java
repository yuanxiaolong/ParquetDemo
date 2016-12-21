package com.yxl.common;

/**
 * 文本文件启动对象
 *
 * Created by xiaolong.yuanxl on 16-12-14.
 */
public class TextInputLaunchDO extends InputLanuchDO{

    private String schema;

    private String sep;//FIXME TODO 暂时没有用到

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSep() {
        return sep;
    }

    public void setSep(String sep) {
        this.sep = sep;
    }

}
