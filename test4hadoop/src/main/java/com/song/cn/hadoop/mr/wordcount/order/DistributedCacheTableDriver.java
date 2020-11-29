package com.song.cn.hadoop.mr.wordcount.order;

import com.song.cn.hadoop.conf.Conf;
import com.song.cn.hadoop.hdfs.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hadoop案例（七）MapReduce中多表合并
 * https://www.cnblogs.com/frankdeng/p/9256248.html
 */
public class DistributedCacheTableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 如果输入输出路径没有传入，直接使用默认的
        if (args.length < 2) {
            args = new String[]{
                    "/hadoop/test/order/input/",
                    "/hadoop/test/order/output/"
            };
        }
        String inputHDFSPath = args[0];
        String outputHDFSPath = args[1];

        // 支持本地上传数据
        String localPath = "D:\\Java\\Hadoop\\test4hadoop\\input\\order\\";
        String fileName1 = "order.txt";
        String fileName2 = "pd.txt";
        Files.uploadFile(localPath, fileName1, inputHDFSPath);
        String distributedFilePath = "/hadoop/test/order/dist/";
        Files.uploadFile(localPath, fileName2, distributedFilePath);

        // 删除输出路径下的数据，避免出错
        Files.deleteFile(outputHDFSPath);

        // 1 获取配置信息，或者 job 对象实例
        Job job = Job.getInstance(Conf.get(), "Order");

        // 2 指定本程序的 jar包所在的本地路径 (顺序不能错)
        job.setJarByClass(DistributedCacheTableDriver.class);

        // 3 指定本业务 job 要使用的mapper/reducer 业务类
        job.setMapperClass(DistributedCacheTableMapper.class);

        // 4 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 指定 job 的输入输出文件所在路径
        FileInputFormat.setInputPaths(job, new Path(inputHDFSPath));
        FileOutputFormat.setOutputPath(job, new Path(outputHDFSPath));

        // 6 加载缓存数据
        job.addCacheFile(
                new URI("hdfs://sparkproject1:9000" + distributedFilePath + fileName2));

        // 7 map 端 join 的逻辑不需要reduce阶段，设置 reduce task 数量为0
        job.setNumReduceTasks(0);

        // 8 将 job 中配置的相关参数，以及 job 所用的java类所在的 jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
