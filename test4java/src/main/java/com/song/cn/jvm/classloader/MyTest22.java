package com.song.cn.jvm.classloader;

/**
 * 将 classes 中的 class 文件 放置到 /Library/Java/JavaVirtualMachines/jdk1.8.0_271.jdk/Contents/Home/jre/classes 下
 * 将使用 启动类加载器 来加载相关的class文件
 */
public class MyTest22 {
    public static void main(String[] args) {
        System.out.println(System.getProperty("sun.boot.class.path"));
        System.out.println(System.getProperty("java.ext.dirs"));
        System.out.println(System.getProperty("java.class.path"));


    }
}
