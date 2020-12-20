package com.song.cn.jvm.classloader;

import java.sql.Driver;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 线程上下文类加载器的一般使用模式：获取 - 使用 - 还原
 * ClassLoader classloader = Thread.currentThread().getContextClassLoader();
 *
 * try{
 *     Thread.currentThread().setContextClassLoader(targetTccl1);
 *     myMethod()
 * }finally{
 *     Thread.currentThread.setContextClassLoader(classloader);
 * }
 * myMethod里面则调用Thread.currentThread().getContextClassLoader(), 获取当前线程的上下文类加载器做某些事情。
 * 如果一个类由类加载器A加载，那么这个类的依赖类也是由相同的类加载器加载的（如果该依赖类之前没有被加载过的话）
 *
 * 在pom.xml中添加 mysql 驱动之后，会输出如下内容
 *  driver: class com.mysql.jdbc.Driver, loader: sun.misc.Launcher$AppClassLoader@18b4aac2
 *  driver: class com.mysql.fabric.jdbc.FabricMySQLDriver, loader: sun.misc.Launcher$AppClassLoader@18b4aac2
 *  当前线程上下文类加载器：sun.misc.Launcher$AppClassLoader@18b4aac2
 *  ServiceLoader的类加载器：null
 */
public class MyTest30 {
    public static void main(String[] args) {
        // 将当前线程上下文类加载器设置成扩展类加载器，则无法找到 MySQL 的驱动
        // 默认 MySQL 驱动是在 classpath下，由系统类加载器加载
        // Thread.currentThread().setContextClassLoader(MyTest30.class.getClassLoader().getParent());
        ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class);
        Iterator<Driver> iterator = loader.iterator();
        while (iterator.hasNext()){
            Driver driver = iterator.next();
            System.out.println("driver: " + driver.getClass()
            + ", loader: " + driver.getClass().getClassLoader());
        }


        System.out.println("当前线程上下文类加载器：" + Thread.currentThread().getContextClassLoader());
        System.out.println("ServiceLoader的类加载器：" + ServiceLoader.class.getClassLoader());
    }
}
