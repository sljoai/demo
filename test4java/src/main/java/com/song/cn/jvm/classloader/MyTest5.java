package com.song.cn.jvm.classloader;

/**
 * 接口成员变量默认为 final，对于static final 的成员变量 会在编译阶段添加到调用者的常量池中
 */
public class MyTest5 {
    public static void main(String[] args) {
        System.out.println(MyChild5.b);
    }
}

interface MyParent5 {
    public static int a =5;
}
interface MyChild5 extends MyParent5 {
    public static int b=6;
}
