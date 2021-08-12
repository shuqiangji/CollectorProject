package com.mobin.config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class HdfsConfigTest {

    @Test
    public void testGetConfig(){
        Boolean test = HdfsConfig.isHa;
        Assert.assertTrue(HdfsConfig.isHa);
        Assert.assertTrue(StringUtils.isNotEmpty(HdfsConfig.nameServices));
        Assert.assertNotNull(HdfsConfig.nameNodes);
        Assert.assertTrue(CollectionUtils.isNotEmpty(HdfsConfig.nameNodes));
        Assert.assertNotNull(HdfsConfig.nameNodesAddress);
        Assert.assertTrue(CollectionUtils.isNotEmpty(HdfsConfig.nameNodesAddress));
        Assert.assertTrue(StringUtils.isNotEmpty(HdfsConfig.hdfsUser));
    }

}