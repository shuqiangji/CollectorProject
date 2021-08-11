package com.mobin.collector;

import com.mobin.config.Config;
import com.mobin.config.HdfsConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Mobin on 2017/5/17.
 */
public class MobinTest {
    @Test
    public void mobintest() throws IOException {
        System.out.println(System.getProperty("mm"));
        System.out.println(MobinTest.class.getResourceAsStream("/File_conf.properties"));
        System.out.println(Config.getStringProperty("Mobin_prefix"));
        File file = new File("E:\\collectProjectFile\\TEST.txt.down");
        //Configuration conf = new Configuration();
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.newInstance(conf);
        CollectorOptions collectorOptions = new CollectorOptions();
        collectorOptions.dateTime = "20170628";
        CollectFile collectFile = new CollectFile(fs, file, "/hdfs/video");
        collectFile.copy();
    }

    @Test
    public void test(){
        System.out.println(System.getProperty("java.io.tmpdir"));
    }

    /**
    * 获取HDFS配置信息
    */
    private Configuration getConfiguration(){
        if (StringUtils.isEmpty(HdfsConfig.nameServices) || CollectionUtils.isEmpty(HdfsConfig.nameNodes)
                || CollectionUtils.isEmpty(HdfsConfig.nameNodesAddress)
                || HdfsConfig.nameNodes.size() != HdfsConfig.nameNodesAddress.size()) {

            throw new RuntimeException("HDFS配置不正确，请检查！");
        }
        Configuration conf = new Configuration();
        String defaultFs = "hdfs://" + HdfsConfig.nameServices;
        conf.set("fs.defaultFS", defaultFs);
        conf.set("dfs.nameservices", HdfsConfig.nameServices);
        String nameNodes = "dfs.ha.namenodes." + HdfsConfig.nameServices;
        conf.set(nameNodes, StringUtils.join(HdfsConfig.nameNodes), ",");
        for (int i = 0; i < HdfsConfig.nameNodes.size(); i++) {
            String key = "dfs.namenode.rpc-address." + HdfsConfig.nameServices + "." + HdfsConfig.nameNodes.get(i);
            conf.set(key, HdfsConfig.nameNodesAddress.get(i));
        }
        conf.set("dfs.client.failover.proxy.provider." + HdfsConfig.nameServices,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }

}
