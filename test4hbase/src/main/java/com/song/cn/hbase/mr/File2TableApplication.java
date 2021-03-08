package com.song.cn.hbase.mr;

import com.song.cn.hbase.mr.tool.File2TableTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

public class File2TableApplication {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new File2TableTool(), args);
        System.exit(run);
    }
}
