package com.song.cn.hadoop.mr.wordcount.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * 寻找共同好友 - Reducer
 */
public class CommonFriendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> countCommonFriend = new HashMap<>();
        Iterator<Text> commonFriendItr = values.iterator();
        // 统计好友出现的次数
        while (commonFriendItr.hasNext()) {
            String friend = commonFriendItr.next().toString();
            String[] friendList = friend.split("\t", -1);
            for (String tmpFriend : friendList) {
                Integer count = countCommonFriend.get(tmpFriend);
                if (count == null) {
                    count = -1;
                }
                countCommonFriend.put(friend, ++count);
            }
        }
        StringBuilder commonFriend = new StringBuilder("");
        // 筛选出有共同好友的记录
        for (Map.Entry<String, Integer> entry : countCommonFriend.entrySet()) {
            if (entry.getValue() > 0) {
                commonFriend.append(entry.getKey()).append("\t");
            }
        }
        context.write(key, new Text(commonFriend.toString()));
    }
}
