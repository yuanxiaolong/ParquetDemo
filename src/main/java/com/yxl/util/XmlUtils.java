package com.yxl.util;

import com.thoughtworks.xstream.XStream;
import com.yxl.common.ParquetInputLaunchDO;
import com.yxl.common.TextInputLaunchDO;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * xml 转换工具
 *
 * Created by xiaolong.yuanxl on 16-12-14.
 */
public class XmlUtils {


    public static TextInputLaunchDO toTextLaunchProp(String path) throws Exception{
        XStream xStream = new XStream();
        xStream.alias("launch",TextInputLaunchDO.class);
        return (TextInputLaunchDO)xStream.fromXML(mkString(path));
    }

    public static ParquetInputLaunchDO toParquetLaunchProp(String path) throws Exception{
        XStream xStream = new XStream();
        xStream.alias("launch",TextInputLaunchDO.class);
        return (ParquetInputLaunchDO)xStream.fromXML(mkString(path));
    }


    private static String mkString(String file) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuffer buff = new StringBuffer();
        String line;
        while((line = br.readLine()) != null){
            buff.append(StringUtils.trim(line));
        }
        return buff.toString();
    }

    public static void main(String[] args) throws Exception{
        TextInputLaunchDO launchDO = toTextLaunchProp("/Users/mfw/Documents/workspace_java/parquetDemo/src/main/resources/demo.xml");
        System.out.println(String.valueOf(launchDO.getSep()));
    }

}
