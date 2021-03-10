package com.song.cn.hadoop.mr.wordcount.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text keyRes = new Text();

    private FlowBean valueRes = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        String phoneNum = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFLow = Long.parseLong(fields[fields.length - 2]);

        keyRes.set(phoneNum);
        valueRes.setUpFlow(upFlow);
        valueRes.setDownFlow(downFLow);
        valueRes.setSumFlow(upFlow + downFLow);

        // 4 写出
        context.write(keyRes,valueRes);
    }
}
