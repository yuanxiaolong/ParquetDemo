package com.yxl.common;

/**
 * hdfs 输入基本启动类
 *
 * Created by xiaolong.yuanxl on 16-12-21.
 */
public abstract class InputLanuchDO {

    protected String input;

    protected String output;

    protected String compress;

    protected boolean isOverwrite = false;

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }

    public void setOverwrite(boolean isOverwrite) {
        this.isOverwrite = isOverwrite;
    }
}
