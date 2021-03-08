package com.song.cn.hbase.mr.mapper;

import com.song.cn.hbase.mr.bean.CacheData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ScanHbaseMapper extends TableMapper<Text, CacheData> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        for (Cell cell : value.rawCells()) {
            String name = Bytes.toString(CellUtil.cloneValue(cell));
            CacheData data = new CacheData();

            data.setName(name);
            data.setCount(1);
            context.write(new Text(name), data);
        }
    }
}
