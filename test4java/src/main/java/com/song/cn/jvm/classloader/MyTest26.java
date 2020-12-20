package com.song.cn.jvm.classloader;

/**
 * 扩展类加载器需要从jar包中加载类
 *
 * ## 将类打成jar包
 * jar cvf test.jar com/song/cn/jvm/classloader/MyTest1.class
 *
 * ## 指定扩展类加载器加载目录，并启动
 * java -Djava.ext.dirs=./ com.song.cn.jvm.classloader.MyTest26
 *  MyTest26 initializer.
 *      sun.misc.Launcher$AppClassLoader@2a139a55
 *      sun.misc.Launcher$ExtClassLoader@33909752
 *
 */
public class MyTest26 {
    static {
        System.out.println("MyTest26 initializer.");
    }

    public static void main(String[] args) {
        System.out.println(MyTest26.class.getClassLoader());
        System.out.println(MyTest1.class.getClassLoader());
    }
}
