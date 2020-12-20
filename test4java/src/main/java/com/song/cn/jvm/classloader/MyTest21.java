package com.song.cn.jvm.classloader;

/**
 * 1.当将MyCat的 class 文件移到指定的路径，MyCat无法被加载，抛出异常
 *      class: 1300109446
 *      MySample is loaded by: sun.misc.Launcher$AppClassLoader@18b4aac2
 *      Exception in thread "main" java.lang.NoClassDefFoundError: com/song/cn/jvm/classloader/MyCat
 *      	at com.song.cn.jvm.classloader.MySample.<init>(MySample.java:6)
 *      	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
 *      	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
 *      	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
 *      	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
 *      	at java.lang.Class.newInstance(Class.java:442)
 *      	at com.song.cn.jvm.classloader.MyTest21.main(MyTest21.java:27)
 *      Caused by: java.lang.ClassNotFoundException: com.song.cn.jvm.classloader.MyCat
 *      	at java.net.URLClassLoader.findClass(URLClassLoader.java:382)
 *      	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
 *      	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355)
 *      	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
 * 2.当将MySample 的 class 文件移到指定的路径下，MySample由自定义的类加载起加载，MyCat由应用类加载器加载
 *      findClass invoked: com.song.cn.jvm.classloader.MySample
 *      class loader name: loader1
 *      com/song/cn/jvm/classloader/MySample
 *      class: 789451787
 *      MySample is loaded by: [loader1]
 *      MyCat is loaded by: sun.misc.Launcher$AppClassLoader@18b4aac2
 * 3.当将 MyCat 和 MySample 的 class 文件移动到指定的路径下， 两者都由自定义类加载器加载
 *
 * 4.在 MyCat中添加对MySample.class的引用  -- 父类加载器看不到子类加载器的类
 *      System.out.println("From MySample: "+ MyCat.class);
 * 5. 在 MySample 中添加 对 MyCat.class 的引用 -- 子类加载器能看到弗雷加载器中的类
 *
 */
public class MyTest21 {
    public static void main(String[] args) throws Exception {
        MyTest19 loader1 = new MyTest19("loader1");
        loader1.setPath("/Users/sljoai/Desktop/Daily/");


        Class<?> clazz = loader1.loadClass("com.song.cn.jvm.classloader.MySample");
        System.out.println("class: " + clazz.hashCode());

        // 如果注释掉该行，那么并不会实例化 MySample 对象；即：MySample构造方法不会被调用；
        // 因此不会实例化MyCat对象，即：没有MyCat进行主动使用，这里不会加载MyCat Class
        // -XX:+TraceClassLoading
        Object object = clazz.newInstance();
    }
}
