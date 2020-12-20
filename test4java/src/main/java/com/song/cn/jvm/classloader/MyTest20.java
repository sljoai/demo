package com.song.cn.jvm.classloader;

public class MyTest20 {
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        MyTest16 loader1 = new MyTest16("loader1");

        Class<?> clazz = loader1.loadClass("com.song.cn.jvm.classloader.MySample");
        System.out.println("class: " + clazz.hashCode());

        // 如果注释掉该行，那么并不会实例化 MySample 对象；即：MySample构造方法不会被调用；
        // 因此不会实例化MyCat对象，即：没有MyCat进行主动使用，这里不会加载MyCat Class
        // -XX:+TraceClassLoading
        Object object = clazz.newInstance();

    }
}
