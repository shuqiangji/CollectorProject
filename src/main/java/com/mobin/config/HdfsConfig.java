package com.mobin.config;

import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

import java.io.*;
import java.util.*;

/**
 * @author JSQ
 * @date 8/11/2021 2:14 PM
 * @description TODO
 * @copyright 2021  All rights reserved.
 */
public class HdfsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfig.class);

    private static final Properties config = loadConfig();

    /**
    * 获取是否HA模式
    */
    public static final Boolean isHa = getIsHa("hdfs.ha");

    /**
    * nameservice
    */
    public static final String nameServices = getConfig("hdfs.dfs.nameservices");

    /**
    * namenode配置
    */
    public static final List<String> nameNodes = getNameNodes("hdfs.ha.namenodes");


    public static final String hdfsUser = getConfig("hdfs.user");

    /**
     * namenode地址配置,ip+port
     */
    public static final List<String> nameNodesAddress = getNameNodes("hdfs.ha.namenodes.address");

    private static Properties loadConfig(){
        InputStream in = null;
        Properties properties = new Properties();
        try {
            in = HdfsConfig.class.getResourceAsStream("/hdfs.properties");
            properties.load(in);
        } catch (IOException e) {
            throw new RuntimeException("hdfs配置文件读取失败！");
        }finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException("hdfs配置文件流关闭异常！");
                }
            }
        }
        return properties;
    }

    private static String getConfig(String key){
        if (null != config){
            String value = config.getProperty(key);
            if (null != value){
                return value.trim();
            }
        }
        return null;
    }

    /**
    * namenode配置字符串转集合
    */
    private static List<String> getNameNodes(String key){
        String nameNodes = getConfig(key);
        if (null == nameNodes){
            return Collections.emptyList();
        }
        String[] nodes = nameNodes.split(",");
        return Arrays.asList(nodes);
    }



    private static Boolean getIsHa(String key){
        String ha = getConfig(key);
        if (null == ha){
            if (ha.toLowerCase() != "true" && ha.toLowerCase() != "false") {
                throw new RuntimeException("hdfs是否HA模式配置错误！");
            }
        }
        boolean isHa = Boolean.parseBoolean(ha);
        return isHa;
    }

}
