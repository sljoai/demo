package com.song.cn.jvm.classloader;

/**
 * 验证：当初始化一个接口时，不会初始化它的父接口
 * 启动参数：-XX:+TraceClassLoading
 * 案例解析：MyParent8 接口会加载，但是不会被初始化
 */
public class MyTest8 {
    public static void main(String[] args) {
        System.out.println(MyChild8.thread);
        // MyChild7 invoked.
        //  Thread[Thread-0,5,main]
    }
}

interface MyParent8 {
    public static final Thread thread = new Thread() {
        {
            System.out.println("MyParent8 invoked.");
        }
    };
}

interface MyChild8 extends MyParent8 {
    public static final Thread thread = new Thread() {
        {
            System.out.println("MyChild7 invoked.");
        }
    };
}
