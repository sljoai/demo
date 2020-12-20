package com.song.cn.jvm.classloader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 1. 指定自定义类加载器的路径，则两个class属于不同的命名空间的；
 * 2. 将 MyPerson.class 移到 其他路径，则会报错
 *  Caused by: java.lang.ClassCastException:
 *      com.song.cn.jvm.classloader.MyPerson cannot be cast to com.song.cn.jvm.classloader.MyPerson
 */
public class MyTest25 {
    public static void main(String[] args) throws IllegalAccessException,
            InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        MyTest17 loader1 = new MyTest17("loader1");
        MyTest17 loader2 = new MyTest17("loader2");
        loader1.setPath("/Users/sljoai/Desktop/Daily/");
        loader2.setPath("/Users/sljoai/Desktop/Daily/");



        Class<?> clazz1 = loader1.loadClass("com.song.cn.jvm.classloader.MyPerson");
        Class<?> clazz2 = loader2.loadClass("com.song.cn.jvm.classloader.MyPerson");

        System.out.println(clazz1 == clazz2); // false

        Object object1 = clazz1.newInstance();

        Object object2 = clazz2.newInstance();

        Method method = clazz1.getMethod("setMyPerson",Object.class);
        method.invoke(object1,object2);

    }
}
