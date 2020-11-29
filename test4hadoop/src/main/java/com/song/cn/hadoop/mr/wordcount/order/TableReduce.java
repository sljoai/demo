package com.song.cn.hadoop.mr.wordcount.order;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 1 准备存储订单的集合
        List<TableBean> orderBeanList = new ArrayList<>();

        // 2 准备 商品 bean 对象
        TableBean productBean = new TableBean();

        for (TableBean bean : values) {
            // 订单表
            if ("0".equals(bean.getFlag())) {
                // 拷贝传递过来的每条订单数据到集合中
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeanList.add(orderBean);
            } else {
                // 产品表
                try {
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(productBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表拼装
        for (TableBean bean : orderBeanList) {
            bean.setpName(productBean.getpName());

            // 4 数据写出
            context.write(bean, NullWritable.get());
        }
    }
}
