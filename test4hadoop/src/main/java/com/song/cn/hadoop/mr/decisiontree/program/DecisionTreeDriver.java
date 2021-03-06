package com.song.cn.hadoop.mr.decisiontree.program;

import com.song.cn.hadoop.conf.Conf;
import com.song.cn.hadoop.hdfs.Files;
import com.song.cn.hadoop.mr.decisiontree.datatype.NodeStatisticInfo;
import com.song.cn.hadoop.mr.decisiontree.datatype.Rule;
import com.song.cn.hadoop.mr.decisiontree.datatype.StatisticRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 整个算法的驱动程序。
 */
public class DecisionTreeDriver {

    // 属性取值的值域范围，数据结构的第一维代表属性ID，
    // 第二维列表表示该属性的所有可能离散取值。
    // 目前程序只能处理离散属性值
    // 这里面不包含Label属性
    static List<List<String>> attributeRange;
    // 类标签取值范围
    static List<String> labelRange;
    // 记录决策树模型，即所有确定的决策规则
    static Queue<Rule> model = new LinkedList<Rule>();
    static Configuration conf = new Configuration();

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) throws Exception {


        args = new String[]{"/hadoop/test/dt/attr/", "/hadoop/test/dt/data/", "/hadoop/test/dt/", "/hadoop/test/dt/model/"};
        // 解析命令行里的额外Hadoop相关参数
        if (args.length < 4) {
            System.out
                    .println("Usage: DecisionTree.jar MetaFile DataSet TmpWorkingDir TargetModelFilePath");
            return;
        }

        // 存储数据属性元信息的文件的路径
        String attributesMetaInfoPath = args[0] + "names";
        //在HDFS上创建数据根目录
        Files.mkdirFolder(args[0]);
        Files.uploadFile("./input_path/dt/attr/", "names", args[0]);

        // 训练集数据文件所在路径
        String dataSetPath = args[1];
        Files.mkdirFolder(dataSetPath);
        Files.uploadFile("./input_path/dt/data/", "train_data", dataSetPath);

        // 当前统计结果所在文件夹的路径
        String statisticFilePath = args[2] + "/static";
        Files.deleteFile(statisticFilePath);

        // 当前层节点队列文件的路径
        String nodeRuleQueueFilePath = args[2] + "/queue";
        Files.deleteFile(nodeRuleQueueFilePath);
        // 模型文件存储路径
        String modelFilePath = args[3] + "model";
        Files.deleteFile(modelFilePath);


        // 载入属性元信息
        loadAttributeRange(attributesMetaInfoPath);
        // 当前层的划分规则队列
        Queue<Rule> currentQueue;
        // 下一层的划分规则队列
        Queue<Rule> newQueue = new LinkedList<Rule>();

        // 是否包含Root节点，用于甄别是否是初始执行
        boolean hasRootNode = false;
        // 当前迭代的轮数，即决策树的深度
        int iterateCount = 0;
        do {
            // 增加一轮迭代
            iterateCount++;
            // 准备当前轮迭代的环境变量数据
            String queueFilePath = nodeRuleQueueFilePath + "/queue-" + iterateCount;
            String newstatisticFilePath =
                    statisticFilePath + "/static-" + iterateCount;
            // 将NewQueue中保存的当前轮迭代所在的层的节点信息写入文件
            outputNodeRuleQueueToFile(newQueue, queueFilePath);
            // 将当前层的队列指针指向上一轮迭代的输出
            currentQueue = newQueue;
            // 判断是否有根节点，对于根节点需要单独处理
            if (!hasRootNode) {
                // 向Queue中插入一个空白的节点，作为根节点
                Rule rule = new Rule();
                currentQueue.add(rule);
                hasRootNode = true;
            }
            // 判断一下当前层数上是否有新的节点可以生长
            if (currentQueue.isEmpty()) {
                // 已经没有新的节点供生长了，决策树模型已经构造完成
                // 退出构造while循环
                break;
            }
            // 继续运行，说明当前层还有节点可供生长
            // 运行MapReduce作业，对当前层节点分裂信息进行统计
            runMapReduceJob(dataSetPath, queueFilePath, newstatisticFilePath,
                    iterateCount);
            // 从输出结果中读取统计好的信息
            Map<Integer, NodeStatisticInfo> nodeStatisticInfos =
                    new HashMap<>();
            loadStatisticInfo(nodeStatisticInfos, newstatisticFilePath);
            // 对于当前层的每个节点进行处理
            int i = 0;
            int Qlength = currentQueue.size();
            for (i = 0; i < Qlength; i++) {
                Rule rule = currentQueue.poll();
                // 节点统计信息里应该包含1到|Q|这|Q|个规则
                // 因为编号从1开始，所以是i+1
                assert (nodeStatisticInfos.containsKey(new Integer(i + 1)));
                NodeStatisticInfo info = nodeStatisticInfos.get(new Integer(i + 1));
                // 获取一些当前节点的统计信息备用
                String mostCommonLabel = info.getMostCommanLabel();
                // 查找最佳分裂属性
                int splitAid = findBestSplit(rule, info);
                System.out.println("BEST_SPLIT_AID:" + splitAid);
                // 如果无法找到最佳分裂属性，即属性分裂不能够提供信息增益了
                // 那么就停止构建新的子节点
                if (splitAid == 0) {
                    // 将“当前规则->当前多数标签”这个规则加入模型中
                    Rule newRule = new Rule();
                    newRule.conditions = new HashMap<Integer, String>(rule.conditions);
                    newRule.label = mostCommonLabel;
                    model.add(newRule);
                    continue;// 继续处理下一个当前层的节点
                }
                // 分裂当前节点
                for (String value : attributeRange.get(splitAid - 1)) {
                    // 判断是否满足终止条件
                    String cLabel = satisfyLeafNodeCondition(rule, info, splitAid, value);
                    // 增加规则
                    Rule newRule = new Rule();
                    newRule.conditions = new HashMap<Integer, String>(rule.conditions);
                    newRule.conditions.put(new Integer(splitAid), value);
                    if (cLabel != null) {
                        // 是叶子节点
                        newRule.label = cLabel;
                        // 将叶节点加入模型
                        model.add(newRule);
                        System.out.println("NEW RULE for label:" + cLabel + " /"
                                + model.size());
                    } else {
                        // 是中间节点，把这个规则加入到新的Queue中
                        newRule.label = "";
                        newQueue.add(newRule);
                    }// end of if
                }// end of for of AttributeValue
            }// end of for of Queue
            System.out.println("NEW QUEUE SIZE:" + newQueue.size());
            writeModelToFile(modelFilePath);
        } while (true);// 不断的向深层扩展决策树，直到无法继续构建为止

    }

    /**
     * 运行MapReduce作业来进行统计
     */
    private static void runMapReduceJob(String dataSetPath,
                                        String nodeRuleQueueFilePath, String statisticFilePath, int itCount)
            throws Exception {
        conf = Conf.get();
        // 配置全局队列
        // 将规则文件加入Cache
        DistributedCache
                .addCacheFile(new Path(nodeRuleQueueFilePath).toUri(), conf);
        System.err.println("NODE_RULE_URI:"
                + new Path(nodeRuleQueueFilePath).toUri());
        org.apache.hadoop.mapreduce.Job job =
                org.apache.hadoop.mapreduce.Job.getInstance(conf, "MR_DecisionTree-" + itCount);
        job.setJarByClass(DecisionTreeDriver.class);
        // 设置Map阶段配置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(DecisionTreeMapper.class);
        // 设置Reduce阶段配置
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(DecisionTreeReducer.class);
        // 设置Reduce节点的个数，这个可以根据实际情况调整
        job.setNumReduceTasks(4);
        // 配置输入输出
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(dataSetPath));
        FileOutputFormat.setOutputPath(job, new Path(statisticFilePath));

        job.waitForCompletion(true);
    }

    /**
     * 判断新增加的子节点是否叶节点
     *
     * @param rule     当前节点规则
     * @param info     当前节点统计信息
     * @param splitAid 属性ID
     * @param value    属性值
     * @return 如果新节点是叶节点，则返回页节点的Label，否则返回null
     */
    private static String satisfyLeafNodeCondition(Rule rule,
                                                   NodeStatisticInfo info, int splitAid, String value) {
        // CASE1:如果新节点不包含任何有效的新元组
        if (info.getRecords(splitAid, value).size() == 0) {
            // 返回当前节点的最长出现的标签
            return info.getMostCommanLabel();
        }
        // CASE2:判断新节点的元组是否都属于同一个类
        List<StatisticRecord> records = info.getRecords(splitAid, value);
        int i = 0;
        for (i = 0; i < records.size() - 1; i++) {
            if (!records.get(i).label.equals(records.get(i + 1).label)) {
                break;
            }
        }
        if (i == records.size() - 1) {
            // 说明正常退出循环，所有标签都相等
            // 则返回这些标签
            return records.get(0).label;
        }
        // CASE3:如果不具有可以再用于分裂的属性
        HashSet<Integer> usedAttributes =
                new HashSet<Integer>(rule.conditions.keySet());
        usedAttributes.add(new Integer(splitAid));
        if (usedAttributes.size() == attributeRange.size()) {
            // 新节点具有的多数类
            List<StatisticRecord> newRecords =
                    info.getRecords(new Integer(splitAid), value);
            return NodeStatisticInfo.getMostCommanLabel(newRecords);
        }
        // 不满足终止条件
        return null;
    }

    /**
     * 从输出结果文件中读取统计信息
     *
     * @param info     用于存储的数据结构
     * @param filePath 统计结果文件所在路径
     *                 <p>
     *                 统计结果文件是由若干行组成，每一行都符合如下的格式约定 "KEY \t COUNT"
     *                 其中KEY中指定了统计的项目，具有如下的格式“nid#aid,属性值,label”。
     *                 nid是节点临时编号（从1开始），aid是属性编号（从1开始），label是类别标签。 COUNT则指符合条件的元组个数。
     */
    public static void loadStatisticInfo(Map<Integer, NodeStatisticInfo> info,
                                         String filePath) {
        try {
            // 从HDFS中读取数据
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] files = fs.listStatus(new Path(filePath));
            Path[] pathes = FileUtil.stat2Paths(files);
            for (Path path : pathes) {
                // 跳过HDFS上的统计文件，这些文件一般以下划线开头
                if (path.getName().startsWith("_"))
                    continue;
                FSDataInputStream inputStream = fs.open(path);
                Scanner scanner = new Scanner(inputStream);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.length() == 0)
                        break;
                    // 获取计数和条件
                    int count = Integer.parseInt(line.split("\\t")[1]);
                    String conditionString = line.split("\\t")[0];
                    Integer nid = new Integer(conditionString.split("#")[0]);
                    Integer aid =
                            new Integer(conditionString.split("#")[1].split("\\,")[0]);
                    String avalue = conditionString.split("#")[1].split("\\,")[1];
                    String label = conditionString.split("#")[1].split("\\,")[2];

                    // 将读入的记录插入已有的数据结构
                    if (!info.containsKey(nid)) {
                        // 如果该记录项的nid之前没出现过，则插入
                        info.put(nid, new NodeStatisticInfo());
                    }
                    NodeStatisticInfo nInfo = info.get(nid);
                    if (nInfo.getAttributeStatisticRecords(aid) == null) {
                        // 如果aid之前没有出现过，则插入新的
                        nInfo.insertAttributeStatisticRecords(aid,
                                new LinkedList<StatisticRecord>());
                    }
                    List<StatisticRecord> recordList =
                            nInfo.getAttributeStatisticRecords(aid);
                    StatisticRecord record =
                            new StatisticRecord(nid, aid, avalue, label, count);
                    recordList.add(record);
                    // System.out.println("RECORD:" + record + "|" +
                    // record.count);
                }// end of while
                scanner.close();
                inputStream.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    // 将队列中的子节点规则信息输出到文件中
    static void outputNodeRuleQueueToFile(Queue<Rule> queue,
                                          String filePath) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            FSDataOutputStream ostream = fs.create(path);
            PrintWriter printWriter = new PrintWriter(ostream);
            // 输出队列中的信息
            for (Rule rule : queue) {
                printWriter.println(rule.toString());
            }
            printWriter.close();
            ostream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }// end of function

    /**
     * 根据rule和统计信息，找到最佳的分裂属性 如果没有好的能带来信息增益的属性，则返回0
     */
    static int findBestSplit(Rule rule, NodeStatisticInfo info) {
        // 假设集合D是满足rule条件的样本集合
        // 计算InfoD,|D|
        Integer DSize = new Integer(0);
        SetInformation sinfo = calcInfoD(info.getRecords(new Integer(0), ""));
        double infoD = sinfo.infoD;
        System.out.println("INFO_D:" + infoD);
        DSize = new Integer(sinfo.size);
        double maxGainRation = 0.0;
        Integer bestSplitAID = null;
        Integer ADSize = new Integer(0);// 一个临时用的double变量
        // 遍历当前节点中每一个可能的候选属性
        for (Integer aid : info.getAvailableAIDSet()) {
            System.out.println("--- " + aid + " ---");
            double infoAD = 0.0;
            double splitInfoAD = 0.0;
            // 计算属性的InfoAD,SplitInfoAD
            for (String value : attributeRange.get(aid.intValue() - 1)) {
                SetInformation sinfoDj = calcInfoD(info.getRecords(aid, value));
                double infoDj = sinfoDj.infoD;
                ADSize = Integer.valueOf(sinfoDj.size);
                assert (!Double.isInfinite(infoDj) && !Double.isNaN(infoDj));
                if (ADSize.intValue() != 0) {
                    infoAD += ADSize.intValue() * infoDj;
                    double p = (double) ADSize.intValue() / (double) DSize.intValue();
                    // System.out.println("FIND_BEST[" + aid + "]" + p);
                    splitInfoAD += -p * Math.log(p) / Math.log(2.0);
                }
            }
            infoAD = infoAD / DSize.intValue();
            System.out.println("InfoAD:" + infoAD + " split:" + splitInfoAD);
            assert (splitInfoAD != 0.0 && splitInfoAD > 0.0);
            // 计算GainA
            double gainA = infoD - infoAD;
            // 计算GainRation
            double gainRatio = gainA / splitInfoAD;
            System.out.println("GainRation[" + aid + "]" + gainA + "/" + splitInfoAD
                    + "=" + gainRatio);
            if (gainRatio > maxGainRation) {
                maxGainRation = gainRatio;
                bestSplitAID = aid;
            }
        }
        if (bestSplitAID == null)
            return 0;
        else
            return bestSplitAID.intValue();
    }

    /**
     * 计算records所包含的样本记录集合的信息熵 该函数同时通过size传回样本集的大小
     */
    private static SetInformation calcInfoD(List<StatisticRecord> records) {
        int sampleCount = 0;
        int[] lcount = new int[labelRange.size()];// 记录每个标签出现了多少次
        double infoD = 0.0;
        for (StatisticRecord record : records) {
            sampleCount += record.count;
            lcount[labelRange.indexOf(record.label)] += record.count;
        }
        if (sampleCount == 0) {
            SetInformation sinfo = new SetInformation();
            return sinfo;// 对于空集合返回0
        }
        for (int i = 0; i < lcount.length; i++) {
            if (lcount[i] != 0) {
                double p = (double) lcount[i] / sampleCount;
                infoD += -p * Math.log(p) / Math.log(2.0);
            }
        }
        SetInformation sinfo = new SetInformation();
        sinfo.size = sampleCount;
        sinfo.infoD = infoD;
        return sinfo;
    }

    /**
     * 从文件中载入属性的取值范围信息 属性文件的每一行代表一个属性值的所有可能取值，
     * 每一行具有如下的格式：“attributeName:value1,value2,...” 其中attributeName是该属性的名称，
     * 后面的value1,value2是可能的取值，每个取值之间使用','分隔。 attributeName可以为空，但是冒号必须保留。
     * 最后一行的属性被认为是label。
     */
    static void loadAttributeRange(String filePath) {
        attributeRange = new ArrayList<List<String>>();
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            FSDataInputStream istream = fs.open(path);
            Scanner scanner = new Scanner(istream);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                List<String> range = new ArrayList<String>();
                String valueList = line.split(":")[1];
                for (String value : valueList.split("\\,")) {
                    range.add(value);
                }
                attributeRange.add(range);
                // System.out.println(line.split(":")[0] + ": " +
                // valueList.toString());
            }
            scanner.close();
            istream.close();
            labelRange = attributeRange.get(attributeRange.size() - 1);
            // 将AttributeRange的最后一个属性从结构里删去
            attributeRange.remove(attributeRange.size() - 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }// end of function

    // 将已有模型写入文件
    public static void writeModelToFile(String filePath) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            FSDataOutputStream ostream = fs.create(path);
            PrintWriter printWriter = new PrintWriter(ostream);
            // 输出队列中的信息
            for (Rule rule : model) {
                printWriter.println(rule.toString());
            }
            printWriter.close();
            ostream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 用于一次返回集合大小和集合的信息熵两个值使用
    static class SetInformation {
        double infoD;
        int size;
    }
}
