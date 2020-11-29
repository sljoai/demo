package com.song.cn.jvm.classloader;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

/**
 *
 */
public class MyTest15 {
    public static void main(String[] args) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        System.out.println(classLoader);

        String resourceName = "com/song/cn/jvm/classloader/MyTest14.class";

        Enumeration<URL> urls = classLoader.getResources(resourceName);

        while (urls.hasMoreElements()){
            System.out.println(urls.nextElement());
        }

        System.out.println("-------------");

        Class<?> clazz = String.class;

        System.out.println(clazz.getClassLoader());


    }
}
