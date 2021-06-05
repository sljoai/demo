package com.song.cn.hadoop.mr.wordcount.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * 寻找共同好友 - Reducer
 */
public class CommonFriendReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text,Text> multipleOutputs;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> countCommonFriend = new HashMap<>();
        Iterator<Text> commonFriendItr = values.iterator();
        // 调试
        /*
        StringBuilder result = new StringBuilder();
        while (commonFriendItr.hasNext()) {
            result.append(commonFriendItr.next()).append("\t");
        }
        context.write(key,new Text(result.toString()));*/
        // 统计好友出现的次数
        while (commonFriendItr.hasNext()) {
            String friend = commonFriendItr.next().toString();
            String[] friendList = friend.split("\t", -1);
            for (String tmpFriend : friendList) {
                Integer count = countCommonFriend.get(tmpFriend);
                if (count == null) {
                    count = -1;
                }
                countCommonFriend.put(tmpFriend, ++count);
            }
        }
        StringBuilder commonFriend = new StringBuilder("");
        // 筛选出有共同好友的记录
        for (Map.Entry<String, Integer> entry : countCommonFriend.entrySet()) {
            if (entry.getValue() > 0) {
                commonFriend.append(entry.getKey()).append("\t");
            }
        }
        // 仅输出有共同好友的关系
        if(!commonFriend.toString().isEmpty()){
            //context.write(key, new Text(commonFriend.toString()));
            multipleOutputs.write(key,new Text(commonFriend.toString()),"common_friend");
        }
    }
}
