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
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphBuilder {
  public static void main(String[] args) throws Exception {
    Configuration conf = Conf.get();

    Job job1 = Job.getInstance(conf, "Graph Builder");
    job1.setJarByClass(GraphBuilder.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setMapperClass(GraphBuilderMapper.class);
    job1.setReducerClass(GraphBuilderReducer.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.waitForCompletion(true);
  }

  /**
   * 得到输出 <FromPage, <1.0 ,ToPage1,ToPage2...>>
   */
  public static class GraphBuilderMapper extends
          Mapper<LongWritable, Text, Text, Text> {
    // 正则表达式，匹配出一对方括号”[]“及其所包含的内容，注意方括号内的内容不含换行符并且至少含有一个字符
    private static final Pattern wikiLinksPatern = Pattern.compile("\\[.+?\\]");

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String pagerank = "1.0\t";
      boolean first = true;
      String[] titleAndText = parseTitleAndText(value);
      String pageName = titleAndText[0];
      Text page = new Text(pageName.replace(',', '_')); // 得到网页的title
      Matcher matcher = wikiLinksPatern.matcher(titleAndText[1]);
      while (matcher.find()) {
        String otherPage = matcher.group();

        // 过滤出只含有wiki内部链接的网页链接//
        otherPage = getWikiPageFromLink(otherPage);
        if (otherPage == null || otherPage.isEmpty())
          continue;
        StringTokenizer itr = new StringTokenizer(otherPage.toString(), "\n");
        for (; itr.hasMoreTokens(); ) {
          if (!first)
            pagerank += ",";
          pagerank += itr.nextToken();
          first = false;
        }

      }
      context.write(page, new Text(pagerank));
    }

    private String[] parseTitleAndText(Text value) throws IOException {
      String[] titleAndText = new String[2];
      int start = value.find("&lttitle&gt");
      start += 11; // 加上start字符串的长度
      int end = value.find("&lt/title&gt", start);
      if (start == -1 || end == -1)
        return new String[]{"", ""};
      titleAndText[0] = Text.decode(value.getBytes(), start, end - start); // getBytes()方法得到字符编码方式

      start = value.find("&lttext xml:space");
      start += 17; // 加上start字符串的长度
      end = value.find("&lt/text&gt", start);
      if (start == -1 || end == -1)
        return new String[]{"", ""};
      titleAndText[1] = Text.decode(value.getBytes(), start, end - start);
      return titleAndText;
    }

    private String getWikiPageFromLink(String aLink) {
      if (isNotWikiLink(aLink))
        return null;

      int start = aLink.startsWith("[[") ? 2 : 1;
      int endLink = aLink.indexOf("]");

      int pipePosition = aLink.indexOf("|");
      if (pipePosition > 0) {
        endLink = pipePosition;
      }

      int part = aLink.indexOf("#");
      if (part > 0) {
        endLink = part;
      }

      aLink = aLink.substring(start, endLink);
      aLink = aLink.replaceAll("\\s", "_"); // 将空白字符（换行、空格等）转换为"_"
      aLink = aLink.replaceAll(",", "");
      if (aLink.contains("&amp;"))
        aLink.replaceAll("&amp", "&");
      return aLink;
    }

    /**
     * 判断是否是wiki百科内部的链接
     **/
    private boolean isNotWikiLink(String aLink) {
      int start = aLink.startsWith("[[") ? 2 : 1;
      if (aLink.length() < start + 2 || aLink.length() > 100)
        return true;
      char firstChar = aLink.charAt(start);

      if (firstChar == '#')
        return true;
      if (firstChar == ',')
        return true;
      if (firstChar == '.')
        return true;
      if (firstChar == '&')
        return true;
      if (firstChar == '\'')
        return true;
      if (firstChar == '-')
        return true;
      if (firstChar == '{')
        return true;
      if (aLink.contains(":"))
        return true;
      if (aLink.contains(","))
        return true;
      if (aLink.contains("&"))
        return true;
      return false;
    }
  }

  public static class GraphBuilderReducer extends
          Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Text value, Context context)
            throws IOException, InterruptedException {
      context.write(key, value);
    }
  }
}

