package com.song.cn.hbase.mr.tool;

import com.song.cn.hbase.mr.mapper.ReadFileMapper;
import com.song.cn.hbase.mr.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class File2TableTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(File2TableTool.class);

        // prepare data: student.csv
        // 10001 zhangsan
        // 10002 lisi
        // 10003 wangwu
        // format
        Path path = new Path("hdfs://hadoop01:8020/data/student.csv");
        FileInputFormat.addInputPath(job,path);

        // mapper
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // reducer
        TableMapReduceUtil.initTableReducerJob("student_hdfs2hbase",
                InsertDataReducer.class,
                job);


        return job.waitForCompletion(true)? JobStatus.State.SUCCEEDED.getValue():
                JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
