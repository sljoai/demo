package com.song.cn.hadoop.mr.wordcount.flatsum;

import com.song.cn.hadoop.conf.Conf;
import com.song.cn.hadoop.hdfs.Files;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FlatSum {

    private final static Logger LOGGER = Logger.getLogger(FlatSum.class);

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) throws Exception {

        String inHdfsPath = "/hadoop/test/flat_sum/input/";

        String localPath = "D:\\Java\\Hadoop\\test4hadoop\\input\\flat_sum\\";
        String fileName = "test.nb";
        Files.uploadFile(localPath, fileName, inHdfsPath);

        String outHdfsPath = "/hadoop/test/flat_sum/output";

        // 删除输出路径下的数据
        Files.deleteFile(outHdfsPath);

        // 获取Job
        Job job = Job.getInstance(Conf.get(), "Add Sum Line");

        // 设置 jar
        job.setJarByClass(FlatSum.class);

        // 设置 Mapper 类
        job.setMapperClass(MapTransfer.class);
        // 设置 Combiner 类
        //job.setCombinerClass(IntSumReducer.class);
        // 设置 Reducer 类
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumberBean.class);

        // 设置 输出 key 值
        job.setOutputKeyClass(NumberBean.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inHdfsPath));
        FileOutputFormat.setOutputPath(job, new Path(outHdfsPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapTransfer extends Mapper<Object, Text, Text, NumberBean> {
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 指定以 "\t" 切割字符串
            String[] keys = value.toString().split("\t");
            if (keys.length > 1) {
                word.set(keys[1]);
            } else {
                word.set(value);
            }
            NumberBean bean = new NumberBean();
            bean.setFirst(Integer.parseInt(keys[0]));
            bean.setSecond(Integer.parseInt(keys[1]));
            bean.setThird(Float.parseFloat(keys[2]));
            System.out.println(bean);
            context.write(word, bean);
            //context.write(word,value);
        }
    }

    public static class SumReducer extends Reducer<Text, NumberBean, NumberBean, Text> {
        @Override
        public void reduce(Text key, Iterable<NumberBean> values, Context context)
                throws IOException, InterruptedException {
            Map<NumberBean, Text> keyResult = new HashMap<>();
            float sum = 0f;
            System.out.println(key);
            // 统计第三列的和
            for (NumberBean value : values) {
                sum += value.getThird();
                NumberBean bean = new NumberBean();
                try {
                    BeanUtils.copyProperties(bean, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                keyResult.put(bean, new Text());
            }
            System.out.println("------ totalNum: " + keyResult.size());
            for (Map.Entry<NumberBean, Text> entry : keyResult.entrySet()) {
                entry.getValue().set(String.valueOf(sum));
                System.out.println("key:" + entry.getKey() + ", value:" + entry.getValue());
                context.write(entry.getKey(), entry.getValue());
            }
        }

/*        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for(Map.Entry<NumberBean,Text> entry:keyResult.entrySet()){
                context.write(entry.getKey(),entry.getValue());
            }
        }*/
    }
}
