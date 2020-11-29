package com.song.cn.jvm.classloader;

public class MyTest14 {
    public static void main(String[] args) {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        System.out.println(classLoader);

        while (null != classLoader){
            classLoader = classLoader.getParent();
            System.out.println(classLoader);
        }
        //sun.misc.Launcher$AppClassLoader@18b4aac2
        //sun.misc.Launcher$ExtClassLoader@4b1210ee
        //null   -> 启动类加载器使用 null 来表示

    }
}
