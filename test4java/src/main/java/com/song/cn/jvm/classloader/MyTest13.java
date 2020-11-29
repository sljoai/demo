package com.song.cn.jvm.classloader;

public class MyTest13 {
    public static void main(String[] args) throws ClassNotFoundException {
        ClassLoader loader = ClassLoader.getSystemClassLoader();// 获取应用类加载起
        Class<?> clazz = loader.loadClass("com.song.cn.jvm.classloader.CL");// 只是加载类，并不会初始化

        System.out.println(clazz);

        System.out.println("------------");

        clazz = Class.forName("com.song.cn.jvm.classloader.CL");
    }
}

class CL{
    static {
        System.out.println("Class CL");
    }
}

