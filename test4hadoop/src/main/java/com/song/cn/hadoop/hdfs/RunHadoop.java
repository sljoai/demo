package com.song.cn.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RunHadoop {

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    private static final String HDFS_SERVER_INFO="hdfs://hadoop01:8020";

    public static void main(String[] args) throws Exception{
        String directory = "/hadoop/test/pagerank";
        //Files.mkdirFolder(directory);
        mkDir();
    }

    /**
     * 在集群上创建目录
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void mkDir() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // 注意：此处的HDFS信息可以从 core-site.xml中寻找 fs.defaultFS标签中查找对应的值
        FileSystem fs = FileSystem.get(new URI(HDFS_SERVER_INFO), configuration, "root");

        // 2 创建目录
        fs.mkdirs(new Path("/data/test/ori"));

        // 3 关闭资源
        fs.close();

    }

    /**
     * 上传文件到集群上
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void uploadFile() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI(HDFS_SERVER_INFO), configuration, "root");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");

    }

    public static void downLoadFile() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI(HDFS_SERVER_INFO), configuration, "root");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);

        // 3 关闭资源
        fs.close();

    }
}
