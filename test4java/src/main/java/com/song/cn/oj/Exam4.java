package com.song.cn.oj;

import java.util.HashMap;
import java.util.Map;

public class Exam4 {

    public static void main(String[] args) {
        String digitalStr = "123312341235123";
        canSearch(digitalStr);

    }

    public static boolean canSearch(String digitalStr) {
        if (null == digitalStr || digitalStr.length() < 4) {
            return false;
        }
        boolean ret = true;
        // 记录从左到右每个索引对应的和
        int[] totalIndexArray = new int[digitalStr.length()];
        // 记录和与索引
        Map<Integer, Integer> sumIndexMap = new HashMap<>();
        int total = 0;
        // 构造从左到右数字之和与索引的关系
        for (int index = 0; index < digitalStr.length(); index++) {
            if (digitalStr.charAt(index) < 48 || digitalStr.charAt(index) > 57) {
                ret = false;
                break;
            }
            //
            total += digitalStr.charAt(index) - 48;
            // 构造索引到和
            totalIndexArray[index] = total;
            // 构造和到索引
            sumIndexMap.put(total, index);
        }
        // 每部分总和不超过总和1/4
        int perTotal = total / 4;
        for (int index = 1; index < digitalStr.length(); index++) {
            if (totalIndexArray[index - 1] > perTotal) {
                ret = false;
                break;
            }
            // 记录第一段总和
            int firstPerTotal = totalIndexArray[index - 1];
            // 记录第二段总和
            int secondPerTotal = totalIndexArray[index] + firstPerTotal;
            // 记录第二段总和索引
            Integer secondIndex = sumIndexMap.get(secondPerTotal);
            if (secondIndex != null) {
                secondIndex += 1;
                // 记录第三段总和
                int thirdPerTotal = totalIndexArray[secondIndex] + firstPerTotal;
                Integer thirdIndex = sumIndexMap.get(thirdPerTotal);
                if (thirdIndex != null) {
                    // 记录第三段总和索引
                    thirdIndex += 1;
                    // 记录第四段总和
                    int fourthPerTotal = totalIndexArray[thirdIndex] + firstPerTotal;

                    if (fourthPerTotal == total) {
                        ret = true;
                        System.out.printf("索引位置 - 1: [ %s] , 2: [ %s] ,3: [ %s ].",index,secondIndex,thirdIndex);
                        break;
                    }
                }
            }
        }
        return ret;
    }
}
