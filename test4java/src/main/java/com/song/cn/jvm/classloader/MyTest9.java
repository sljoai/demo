package com.song.cn.jvm.classloader;

public class MyTest9 {
    public static void main(String[] args) throws ClassNotFoundException {
        Class<?> clazz = Class.forName("java.lang.String");
        System.out.println(clazz.getClassLoader());
        // null，String的类加载起是Bootstrap
        Class<?> clazzC = Class.forName("com.song.cn.jvm.classloader.C");
        System.out.println(clazzC.getClassLoader());
        // sun.misc.Launcher$AppClassLoader@18b4aac2
        // 应用类加载起

    }
}

class C {

}
