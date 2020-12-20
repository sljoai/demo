package com.song.cn.jvm.classloader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MyTest24 {
    public static void main(String[] args) throws IllegalAccessException,
            InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        MyTest16 loader1 = new MyTest16("loader1");
        MyTest16 loader2 = new MyTest16("loader2");

        Class<?> clazz1 = loader1.loadClass("com.song.cn.jvm.classloader.MyPerson");
        Class<?> clazz2 = loader2.loadClass("com.song.cn.jvm.classloader.MyPerson");

        System.out.println(clazz1 == clazz2); // true

        Object object1 = clazz1.newInstance();

        Object object2 = clazz2.newInstance();

        Method method = clazz1.getMethod("setMyPerson",Object.class);
        method.invoke(object1,object2);

    }
}
