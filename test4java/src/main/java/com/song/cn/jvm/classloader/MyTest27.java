package com.song.cn.jvm.classloader;

import sun.misc.Launcher;

/**
 * 指定 系统类加载器
 * java -Djava.system.class.loader=com.song.cn.jvm.classloader.MyTest16 com.song.cn.jvm.classloader.MyTest27
 */
public class MyTest27 {
    public static void main(String[] args) {
        System.out.println(System.getProperty("sun.boot.class.path"));
        System.out.println(System.getProperty("java.ext.dirs"));
        System.out.println(System.getProperty("java.class.path"));
        // ClassLoader 是由启动类加载器加载的
        System.out.println(ClassLoader.class.getClassLoader());
        // 扩展类加载器和系统类加载器也是由启动类加载器加载的
        System.out.println(Launcher.class.getClassLoader());

        System.out.println("----------------");
        // 可以通过 java.system.class.loader  指定 系统类加载器，默认为为null
        System.out.println(System.getProperty("java.system.class.loader"));

        System.out.println(MyTest27.class.getClassLoader());
        System.out.println(MyTest16.class.getClassLoader());

        // TODO: 此处有点问题，与预期（MyTest16）的不一致
        System.out.println(ClassLoader.getSystemClassLoader());
    }
}
