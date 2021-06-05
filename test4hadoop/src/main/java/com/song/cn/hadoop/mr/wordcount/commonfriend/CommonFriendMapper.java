package com.song.cn.hadoop.mr.wordcount.commonfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 寻找共同好友 - mapper
 */
public class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String relations = value.toString();
        String[] relationList = relations.split(" ", -1);
        // 当关系数组为2时，表示没有共同的好友
        if (relationList.length < 3) {
            return;
        }
        for (int i = 1; i < relationList.length; i++) {
            String otherRelation = relationList[i];
            String relationName = otherRelation.compareToIgnoreCase(relationList[0]) > 0 ?
                    relationList[0] + "_" + otherRelation : otherRelation + "_" + relationList[0];

            // 记录共同好友
            StringBuilder commonFriend = new StringBuilder();
            for (int j = 1; j < relationList.length; j++) {
                if (j == i) {
                    continue;
                }
                if (j == relationList.length - 1) {
                    commonFriend.append(relationList[j]);
                } else {
                    commonFriend.append(relationList[j]).append("\t");
                }
            }
            // 写出结果
            context.write(new Text(relationName), new Text(commonFriend.toString()));
        }
    }
}
