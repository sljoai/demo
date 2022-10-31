package com.song.cn.oj;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Exam2 {
    public static void main(String[] args) {
        int[] dataArr = new int[]{100,101,102,103,104,105};
        int[] retDataArr = new int[dataArr.length];
        String file= "Dest.txt";

        // 写入数据
        DataOutputStream dos;
        try {
            dos = new DataOutputStream(new FileOutputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        for(int data:dataArr){
            try {
                dos.writeInt(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 读取数据
        DataInputStream dis = null;
        try {
            dis = new DataInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        // 读取之后逆序存储在数组中
        for(int i=retDataArr.length-1;i>=0;i--){
            try {
                retDataArr[i] = dis.readInt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            dis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 逆序输出
        for(int i=0;i<retDataArr.length;i++){
            System.out.println(retDataArr[i]);
        }
    }
}
