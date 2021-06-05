package com.song.cn.hadoop.mr.wordcount.commonfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 计算共同好友-Driver端
 * 1. 将jar包上传服务器之后，需要对jar进行特殊处理
 * zip -d test4hadoop.jar 'META-INF/.SF' 'META-INF/.RSA' 'META-INF/*SF'
 * 另外，针对基于pom.xml文件使用mvn进行打包的过程，可以添加shade插件，在打包的时候排除.SF等文件
 * 2. 提交任务
 * hadoop jar test4hadoop.jar com.song.cn.hadoop.mr.wordcount.commonfriend.CommonFriendDriver \
 * /data/test/input/common_friend.txt \
 * /data/test/output
 * 3. 查看结果
 * hadoop fs -cat /data/test/output/part-r-00000
 */
public class CommonFriendDriver {
    public static void main(String[] args) throws Exception {
        // 如果输入输出路径没有传入，直接使用默认的
        if (args.length < 2) {
            System.err.println("未找到输入路径或输出路径参数！");
            System.exit(1);
        }
        String inputHDFSPath = args[0];
        String outputHDFSPath = args[1];

        // 获取连接配置
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        FileSystem fs = FileSystem.get(conf);
        // 删除输出路径下的数据，避免出错
        fs.delete(new Path(outputHDFSPath), true);


        // 1 获取配置信息，或者 job 对象实例
        Job job = Job.getInstance(conf, "FindCommonFriend");

        // 6 指定本程序的 jar包所在的本地路径
        job.setJarByClass(CommonFriendDriver.class);

        // 取消类似part-r-00000的空文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // 2 指定本业务 job 要使用的mapper/reducer 业务类
        job.setMapperClass(CommonFriendMapper.class);
        job.setReducerClass(CommonFriendReducer.class);

        // 3 指定 mapper 输出数据的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 5 指定 job 的输入输出文件所在路径
        FileInputFormat.setInputPaths(job, new Path(inputHDFSPath));
        FileOutputFormat.setOutputPath(job, new Path(outputHDFSPath));


        // 7 将 job 中配置的相关参数，以及 job 所用的java类所在的 jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
