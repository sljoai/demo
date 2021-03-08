package com.song.cn.hbase.mr.tool;

import com.song.cn.hbase.mr.bean.CacheData;
import com.song.cn.hbase.mr.mapper.ScanHbaseMapper;
import com.song.cn.hbase.mr.reducer.HBase2MySQLReducer;
import com.song.cn.hbase.mr.reducer.MySQLOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBase2MySQLTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HBase2MySQLTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob("student",
                new Scan(),
                ScanHbaseMapper.class,
                Text.class,
                CacheData.class,
                job
        );

        // reducer
        job.setReducerClass(HBase2MySQLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CacheData.class);

        job.setOutputFormatClass(MySQLOutputFormat.class);

        return job.waitForCompletion(true) ?
                JobStatus.State.SUCCEEDED.getValue() :
                JobStatus.State.FAILED.getValue();
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void setConf(Configuration conf) {

    }
}
