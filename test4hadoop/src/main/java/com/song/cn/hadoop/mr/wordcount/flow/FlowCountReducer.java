package com.song.cn.hadoop.mr.wordcount.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFLow = 0;
        long sumDownFLow = 0;

        // 1 遍历所有Bean，将其中的上行流量、下行流量分别累加
        for (FlowBean flowBean : values) {
            sumDownFLow += flowBean.getDownFlow();
            sumUpFLow += flowBean.getUpFlow();
        }

        // 2 封装对象
        FlowBean resultBean = new FlowBean(sumUpFLow, sumDownFLow);

        // 3 写出
        context.write(key, resultBean);
    }
}
