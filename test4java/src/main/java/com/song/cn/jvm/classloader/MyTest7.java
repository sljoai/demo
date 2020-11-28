package com.song.cn.jvm.classloader;

/**
 * 验证：当初始化一个类时，不会初始化它所实现的接口
 * 启动参数：-XX:+TraceClassLoading
 * 案例解析：MyParent7 接口会加载，但是不会被初始化
 */
public class MyTest7 {
    public static void main(String[] args) {
        System.out.println(MyChild7.b);
        // [Loaded com.song.cn.jvm.classloader.MyParent7 from file:/Users/sljoai/Workspace/Java/demo/test4java/target/classes/]
        // [Loaded com.song.cn.jvm.classloader.MyParent7$1 from file:/Users/sljoai/Workspace/Java/demo/test4java/target/classes/]
        // [Loaded com.song.cn.jvm.classloader.MyChild7$1 from file:/Users/sljoai/Workspace/Java/demo/test4java/target/classes/]
        //  MyChild7 invoked.
        //  7
    }
}
interface MyParent7 {
    public static final Thread thread = new Thread(){
        {
            System.out.println("MyParent7 invoked.");
        }
    };
}

class MyChild7 implements MyParent7{
    public static final Thread thread =  new Thread(){
        {
            System.out.println("MyChild7 invoked.");
        }
    };
    public static int b=7;
}
