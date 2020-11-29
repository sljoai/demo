package com.song.cn.hadoop.mr.wordcount.order;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheTableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    /**
     * 存储 商品ID 和 其名称
     */
    private Map<String, String> productMap = new HashMap();

    private Text keyRes = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存文件
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream("pd.txt"), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            productMap.put(fields[0], fields[1]);
        }
        // 4 关流
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split("\t");

        // 3 获取产品id
        String pId = fields[1];

        // 4 获取商品名称
        String pdName = productMap.get(pId);

        // 5 拼接
        keyRes.set(line + "\t" + pdName);

        // 6 写出
        context.write(keyRes, NullWritable.get());
    }
}
