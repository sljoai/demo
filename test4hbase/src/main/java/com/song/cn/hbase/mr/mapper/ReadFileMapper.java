package com.song.cn.hbase.mr.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取 HDFS 上的数据的Map过程
 */
public class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\\t");
        String rowKey = splits[0];
        byte[] bytes = Bytes.toBytes(rowKey);
        Put put = new Put(bytes);

        String family = "info";
        String column = "name";
        String name = splits[1];
        put.addColumn(Bytes.toBytes(family),
                Bytes.toBytes(column),
                Bytes.toBytes(name));

        context.write(new ImmutableBytesWritable(bytes),put);

    }
}
