package com.song.cn.hadoop.hdfs;

import org.apache.log4j.PropertyConfigurator;

public class RunHadoop {

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) {
        String directory = "/hadoop/test/pagerank";
        Files.mkdirFolder(directory);
    }
}
