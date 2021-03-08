package com.song.cn.hbase.mr;

import com.song.cn.hbase.mr.tool.HBase2MySQLTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

public class HBase2MySQLApplication {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new HBase2MySQLTool(), args);
        System.exit(run);
    }
}
