package com.song.cn.hadoop.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class SimpleInvertedIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "invert index");
        job.setJarByClass(SimpleInvertedIndex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 对文本进行处理，得到<word,filename#offset>格式的键值对输出，从而得到一个单词在文档中出现的位置
     **/
    public static class InvertedIndexMapper extends
            Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName(); // 得到文件名
            Text word = new Text();
            Text fileName_lineOffset = new Text(fileName + "#" + key.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens(); ) {
                word.set(itr.nextToken());
                context.write(word, fileName_lineOffset);
            }
        }
    }

    /**
     * 从Mapper处得到的内容，根据相同key值，进行累加处理，输出该单词所有出现的文档位置
     **/
    public static class InvertedIndexReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            if (it.hasNext())
                all.append(it.next().toString());
            for (; it.hasNext(); ) {
                all.append(";");
                all.append(it.next().toString());
            }
            context.write(key, new Text(all.toString()));
        } // 最终输出键值对示例：("fish", "doc1#0; doc1#8;doc2#0;doc2#8 ")
    }
}
