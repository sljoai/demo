package com.song.cn.jvm.classloader;

public class MyTest2 {
    public static void main(String[] args) {
        System.out.println(MyParent2.str);
    }
}

class MyParent2{
    //public static String str = "hello world";
    public static final String str = "hello world";

    static {
        System.out.println("MyParent2 static block");
    }
}
