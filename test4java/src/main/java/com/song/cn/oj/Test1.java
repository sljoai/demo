package com.song.cn.oj;

import java.util.Scanner;
public class Test1 {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String line=in.next();
        String[] numbers = line.split(" ",-1);
        int start=-1;

        // 找到第一个有实际输入的数字
        for(int i=0;i<numbers.length;i++){
            if("1".equals(numbers[i])){
                start=i;
                break;
            }
        }

        // 记录最后输出结果
        int ans=0;
        if(start==-1){
            System.out.println(ans);
            return;
        }
        // 如果不包含5，则只记录1出现的数量，作为最终的结果
        int hasCtrlAll=-1;
        for(int i=0;i<numbers.length-1;i++){
            if("1".equals(numbers[i])){
                ans++;
            }
            if("5".equals(numbers[i])){
                if("2".equals(numbers[i+1])||"3".equals(numbers[i+1])){
                    // 找到第一个可以得到剪切板包含值的情况
                    hasCtrlAll=i;
                    break;
                }else if("1".equals(numbers[i+1])){
                    ans=1;
                }
            }
        }
        // 未找到一个可以得到剪切板包含值的情况
        if(hasCtrlAll<0){
            System.out.println(ans);
            return;
        }

        if("3".equals(numbers[hasCtrlAll+1])){
            ans=0;
        }
        int m=hasCtrlAll+2;
        // 剪切板长度
        int hasNumInCutbar=ans;
        while(m<numbers.length-1){
            // 当遇到ctrl-a，且下一个是ctrl-x或ctrl-c时
            if(numbers[m].equals("5")&&(numbers[m].equals("2")|| numbers[m].equals("3"))){
                hasNumInCutbar=ans;
                ans=0;
                m=m+2;
                continue;
            }
            // 当为crtl-v时，若长度为0，则ans直接等于剪切板长度
            if(numbers[m].equals("4")){
                ans+=hasNumInCutbar;
            }
            if(numbers[m].equals("1")){
                ans+=1;
            }
            m++;
        }

        System.out.println(ans);
    }
}
