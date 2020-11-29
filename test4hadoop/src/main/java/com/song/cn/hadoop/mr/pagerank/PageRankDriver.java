package com.song.cn.hadoop.mr.pagerank;


import com.song.cn.hadoop.hdfs.Files;
import org.apache.log4j.PropertyConfigurator;

public class PageRankDriver {

    private static int times = 10; // 设置迭代次数

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/hadoop/test/pagerank/input/";
        Files.deleteFile(inputPath);
        String outPath = "/hadoop/test/pagerank/output";
        Files.mkdirFolder(inputPath);

        String localPath = "D:\\Java\\SRC\\test\\input\\";
        Files.uploadFile(localPath, "wiki_0.txt", inputPath);

        Files.deleteFile(outPath);

        String[] forGB = {"", outPath + "/Data0"};
        forGB[0] = inputPath;
        GraphBuilder.main(forGB);

        String[] forItr = {"", ""};
        for (int i = 0; i < times; i++) {
            forItr[0] = outPath + "/Data" + i;
            forItr[1] = outPath + "/Data" + String.valueOf(i + 1);
            PageRankIter.main(forItr);
        }

        String[] forRV = {outPath + "/Data" + times, outPath + "/FinalRank"};
        PageRankViewer.main(forRV);
    }
}

