package com.song.cn.hbase.mr.tool;

import com.song.cn.hbase.mr.mapper.ScanDataMapper;
import com.song.cn.hbase.mr.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBaseMapperReduceTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 作业
        Job job = Job.getInstance();
        job.setJarByClass(HBaseMapperReduceTool.class);

        Scan scan = new Scan();
        // mapper
        TableMapReduceUtil.initTableMapperJob("student",
                scan,
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,job);

        // reducer
        TableMapReduceUtil.initTableReducerJob(
                "student2",
                InsertDataReducer.class,
                job
        );

        // 执行作业
        boolean flag = job.waitForCompletion(true);

        return flag ? JobStatus.State.SUCCEEDED.getValue()
                : JobStatus.State.FAILED.getValue();
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void setConf(Configuration conf) {

    }
}
