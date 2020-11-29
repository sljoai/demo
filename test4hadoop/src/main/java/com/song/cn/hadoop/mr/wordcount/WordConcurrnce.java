package com.song.cn.hadoop.mr.wordcount;

import com.song.cn.hadoop.conf.Conf;
import com.song.cn.hadoop.hdfs.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 统计在若干篇文档中两个英文单词在一定窗口内同时出现的次数
 *
 * @author KING
 */
public class WordConcurrnce {

    private static int MAX_WINDOW = 20;//单词同现的最大窗口大小
    private static String wordRegex = "([a-zA-Z]{1,})";//仅仅匹配由字母组成的简单英文单词
    private static Pattern wordPattern = Pattern.compile(wordRegex);//用于识别英语单词(带连字符-)
    private static IntWritable one = new IntWritable(1);

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String localPath = "D:\\Java\\SRC\\test\\input\\";
        String fileName1 = "test1.txt";
        String fileName2 = "test2.txt";
        String inHdfsPath = "/hadoop/test/input/";
        Files.uploadFile(localPath, fileName1, inHdfsPath);
        Files.uploadFile(localPath, fileName2, inHdfsPath);

        String outHdfsPath = "/hadoop/test/output/";
        Files.deleteFile(outHdfsPath);

        Job wordConcurrenceJob = Job.getInstance(Conf.get());
        wordConcurrenceJob.setJobName("wordConcurrenceJob");
        wordConcurrenceJob.setJarByClass(WordConcurrnce.class);
        wordConcurrenceJob.getConfiguration().setInt("window", 20);

        wordConcurrenceJob.setMapperClass(WordConcurrenceMapper.class);
        wordConcurrenceJob.setMapOutputKeyClass(WordPair.class);
        wordConcurrenceJob.setMapOutputValueClass(IntWritable.class);

        wordConcurrenceJob.setReducerClass(WordConcurrenceReducer.class);
        wordConcurrenceJob.setOutputKeyClass(WordPair.class);
        wordConcurrenceJob.setOutputValueClass(IntWritable.class);

        wordConcurrenceJob.setInputFormatClass(WholeFileInputFormat.class);
        wordConcurrenceJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(wordConcurrenceJob, new Path(inHdfsPath));
        FileOutputFormat.setOutputPath(wordConcurrenceJob, new Path(outHdfsPath));

        wordConcurrenceJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public static class WordConcurrenceMapper extends Mapper<Text, BytesWritable, WordPair, IntWritable> {
        private int windowSize;
        private Queue<String> windowQueue = new LinkedList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            windowSize = Math.min(context.getConfiguration().getInt("window", 2), MAX_WINDOW);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
        }

        /**
         * 输入键位文档的文件名，值为文档中的内容的字节形式。
         */
        @Override
        public void map(Text docName, BytesWritable docContent, Context context) throws
                IOException, InterruptedException {
            Matcher matcher = wordPattern.matcher(new String(docContent.getBytes(), "UTF-8"));
            while (matcher.find()) {
                windowQueue.add(matcher.group());
                if (windowQueue.size() >= windowSize) {
                    //对于队列中的元素[q1,q2,q3...qn]发射[(q1,q2),1],[(q1,q3),1],
                    //...[(q1,qn),1]出去
                    Iterator<String> it = windowQueue.iterator();
                    String w1 = it.next();
                    while (it.hasNext()) {
                        String next = it.next();
                        context.write(new WordPair(w1, next), one);
                    }
                    windowQueue.remove();
                }
            }
            if (!(windowQueue.size() <= 1)) {
                Iterator<String> it = windowQueue.iterator();
                String w1 = it.next();
                while (it.hasNext()) {
                    context.write(new WordPair(w1, it.next()), one);
                }
            }
        }

    }

    public static class WordConcurrenceReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
        @Override
        public void reduce(WordPair wordPair, Iterable<IntWritable> frequence, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : frequence) {
                sum += val.get();
            }
            context.write(wordPair, new IntWritable(sum));
        }
    }
}
