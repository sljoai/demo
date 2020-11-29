package com.song.cn.hadoop.mr.pagerank;

import com.song.cn.hadoop.conf.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankIter {
  private static final double damping = 0.85;

  public static void main(String[] args) throws Exception {
    Configuration conf = Conf.get();
    Job job2 = Job.getInstance(conf, "PageRankIter");
    job2.setJarByClass(PageRankIter.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setMapperClass(PRIterMapper.class);
    job2.setReducerClass(PRIterReducer.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job2.waitForCompletion(true);
  }

  public static class PRIterMapper extends
          Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String line = value.toString();
      String[] tuple = line.split("\t");
      String pageKey = tuple[0];
      double pr = Double.parseDouble(tuple[1]);

      if (tuple.length > 2) {
        String[] linkPages = tuple[2].split(",");
        for (String linkPage : linkPages) {
          String prValue =
                  pageKey + "\t" + String.valueOf(pr / linkPages.length);
          context.write(new Text(linkPage), new Text(prValue));
        }
        context.write(new Text(pageKey), new Text("|" + tuple[2]));
      }
    }
  }

  public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      String links = "";
      double pagerank = 0;
      for (Text value : values) {
        String tmp = value.toString();

        if (tmp.startsWith("|")) {
          links = "\t" + tmp.substring(tmp.indexOf("|") + 1);// index从0开始
          continue;
        }

        String[] tuple = tmp.split("\t");
        if (tuple.length > 1)
          pagerank += Double.parseDouble(tuple[1]);
      }
      pagerank = (double) (1 - damping) + damping * pagerank; // PageRank的计算迭代公式
      context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
    }

  }
}
