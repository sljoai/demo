package com.song.cn.hbase.mr;

import com.song.cn.hbase.mr.tool.HBaseMapperReduceTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

public class Table2TableApplication {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new HBaseMapperReduceTool(), args);
        System.exit(run);

    }
}
