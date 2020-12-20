package com.song.cn.jvm.classloader;

import com.sun.crypto.provider.AESKeyGenerator;

/**
 * 1.修改 -Djava.ext.dirs=./ 环境变量 为 ./
 *  若使用 IDEA 来进行启动，则会使用应用类加载器进行启动
 *      sun.misc.Launcher$AppClassLoader@18b4aac2
 *      sun.misc.Launcher$AppClassLoader@18b4aac2
 *    应用类加载器默认会加载
 *    /Library/Java/JavaVirtualMachines/jdk1.8.0_271.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar
 *  若使用 控制台 来启动，则会报错
 *    java -Djava.ext.dirs=./ com.song.cn.jvm.classloader.MyTest23
 *      Caused by: java.lang.ClassNotFoundException: com.sun.crypto.provider.AESKeyGenerator
 */
public class MyTest23 {
    public static void main(String[] args) {
        System.out.println(System.getProperty("sun.boot.class.path"));
        System.out.println(System.getProperty("java.ext.dirs"));
        System.out.println(System.getProperty("java.class.path"));

        AESKeyGenerator aesKeyGenerator = new AESKeyGenerator();

        System.out.println(aesKeyGenerator.getClass().getClassLoader());
        System.out.println(MyTest23.class.getClassLoader());
    }
}
