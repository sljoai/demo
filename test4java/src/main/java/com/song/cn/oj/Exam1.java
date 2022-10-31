package com.song.cn.oj;

import java.util.ArrayList;
import java.util.List;

public class Exam1 {

    private static String NG = "NG";
    private static String OK = "OK";
    private static int MIN_LENGTH = 2;

    public static void main(String[] args) {
        // 获取键盘输入的字符串
        List passList = new ArrayList();
        passList.add("2021Abc9000");
        passList.add("2021Abc9Abc1");
        passList.add("2021ABC9000");
        passList.add("2021$bc9000");

        judge(passList);
    }

    public static void judge(List<String> passList) {
        if (null == passList || passList.size() < 0) {
            return;
        }
        for (String pass : passList) {
            System.out.println(judge(pass));
        }
    }

    public static String judge(String password) {
        String ret = OK;
        // 判断字符串长度是否合法
        if (null == password || password.length() <= MIN_LENGTH) {
            return NG;
        }

        // 判断字符类型是否合法
        boolean temp = isTypeLegal(password);
        if (!temp) {
            return NG;
        }
        // 判断字母字符是否有重复
        temp = isCharDifferent(password);
        if (!temp) {
            return NG;
        }

        return ret;
    }

    private static boolean isTypeLegal(String pass) {
        boolean ret = false;
        boolean isBig = false;
        boolean isSmall = false;
        boolean isDigital = false;
        boolean isSpecial = false;
        int count = 0;
        for (int i = 0; i < pass.length(); i++) {
            //判断大写字母
            if ((int) pass.charAt(i) > 64 && (int) pass.charAt(i) < 91) {
                if (!isBig){
                    count++;
                }
                isBig = true;
            }
            //判断小写字母
            else if ((int) pass.charAt(i) > 96 && (int) pass.charAt(i) < 123) {
                if (!isSmall){
                    count++;
                }
                isSmall = true;
            }
            //判断数字
            else if ((int) pass.charAt(i) > 47 && (int) pass.charAt(i) < 58) {
                if(!isDigital){
                    count++;
                }
                isDigital = true;
            }
            //判断其他字符个数
            else{
                if(!isSpecial){
                    count++;
                }
                isSpecial = true;

            }
            if (count >= 3) {
                ret = true;
                break;
            }
        }
        return ret;
    }

    /**
     * 判断字符串中是否包含重复字母字符
     *
     * @param pass
     * @return
     */
    private static boolean isCharDifferent(String pass) {
        boolean ret = true;
        for (int i = 0; i < pass.length(); i++) {
            int count = 1;
            // 排除数字
            if (pass.charAt(i) > 47 && (int) pass.charAt(i) < 58) {
                continue;
            }
            for (int j = i + 1; j < pass.length(); j++) {
                if (pass.charAt(i) > 47 && (int) pass.charAt(i) < 58) {
                    continue;
                }
                if (pass.charAt(i) == pass.charAt(j)) {
                    count++;
                    break;
                }
            }
            if (count > 1) {
                ret = false;
                break;
            }
        }
        return ret;
    }
}


