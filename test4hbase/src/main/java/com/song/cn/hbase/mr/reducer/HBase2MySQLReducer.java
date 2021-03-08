package com.song.cn.hbase.mr.reducer;

import com.song.cn.hbase.mr.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HBase2MySQLReducer extends Reducer<Text, CacheData, Text, CacheData> {
    @Override
    protected void reduce(Text key, Iterable<CacheData> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (CacheData cacheData : values) {
            sum += cacheData.getCount();
        }
        CacheData cacheData = new CacheData();
        cacheData.setName(key.toString());
        cacheData.setCount(sum);

        context.write(key,cacheData);
    }
}
