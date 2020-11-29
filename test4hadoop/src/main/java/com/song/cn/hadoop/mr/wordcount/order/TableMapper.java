package com.song.cn.hadoop.mr.wordcount.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private TableBean valueRes = new TableBean();

    private Text keyRes = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取输入文件类型
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        // 2 获取输入数据
        String line = value.toString();

        // 3 不同文件分别处理
        // 3.1 切割
        String[] fields = line.split("\t");
        if (name.startsWith("order")) {
            // 订单

            // 3.2 封装bean对象
            valueRes.setOrderId(fields[0]);
            valueRes.setpId(fields[1]);
            valueRes.setAmount(Integer.parseInt(fields[2]));
            valueRes.setpName("");
            valueRes.setFlag("0");

            keyRes.set(fields[1]);
        } else {
            // 产品
            // 3.2 封装 bean 对象
            valueRes.setpId(fields[0]);
            valueRes.setpName(fields[1]);
            valueRes.setFlag("1");
            valueRes.setAmount(0);
            valueRes.setOrderId("");

            keyRes.set(fields[0]);
        }
        // 4 写出
        context.write(keyRes, valueRes);
    }
}
