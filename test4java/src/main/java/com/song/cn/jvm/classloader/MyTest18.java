package com.song.cn.jvm.classloader;

import java.io.*;

/**
 * 1. 删除 target/classes 下的 MyTest1.class 文件
 * 2. 执行
 * 会发现 两个 classloader 会输出不同的内容
 *  findClass invoked: com.song.cn.jvm.classloader.MyTest1
 * class loader name: loader1
 * com/song/cn/jvm/classloader/MyTest1
 * class: 1020371697
 * object:com.song.cn.jvm.classloader.MyTest1@2f0e140b
 * parent: [loader1]
 * --------------------
 * findClass invoked: com.song.cn.jvm.classloader.MyTest1
 * class loader name: loader2
 * com/song/cn/jvm/classloader/MyTest1
 * class: 2016447921
 * object:com.song.cn.jvm.classloader.MyTest1@27c170f0
 * parent: [loader2]
 * 重新编译之后，两者是一样的
 * findClass invoked: com.song.cn.jvm.classloader.MyTest1
 * class loader name: loader1
 * com/song/cn/jvm/classloader/MyTest1
 * class: 1020371697
 * object:com.song.cn.jvm.classloader.MyTest1@2f0e140b
 * parent: [loader1]
 * --------------------
 * class: 1020371697
 * object:com.song.cn.jvm.classloader.MyTest1@7440e464
 * parent: [loader1]
 */
public class MyTest18 extends ClassLoader {

    private String classLoaderName;

    private final String fileExtention = ".class";

    private String path;

    public MyTest18(String classLoaderName) {
        super(); // 将系统类加载器当作该类加载器的父加载器
        this.classLoaderName = classLoaderName;
    }

    public MyTest18(ClassLoader parent, String classLoaderName) {
        super(parent); // 显示指定该类加载器的父加载器
        this.classLoaderName = classLoaderName;
    }

    public void setPath(String path){
        this.path = path;
    }

    @Override
    public String toString() {
        return "[" + classLoaderName + "]";
    }

    /**
     * 根据 完整 className 加载类文件字节数组
     * @param fullClassName 完整类名
     * @return  类字节码数据
     */
    private byte[] loadClassData(String fullClassName) {
        InputStream is =null;
        byte[] data =null;
        ByteArrayOutputStream baos = null;

        try {
            //TODO: 未调用
            String clazzFileName = fullClassName.replace(".", "/");
            is = new FileInputStream(new File(path +clazzFileName + this.fileExtention));
            System.out.println(clazzFileName);
            /*this.classLoaderName = this.classLoaderName.replace(".","/");
            is = new FileInputStream(new File(fullClassName+this.fileExtention));*/
            baos = new ByteArrayOutputStream();

            int ch = 0;
            while (-1 != (ch = is.read())) {
                baos.write(ch);
            }
            data = baos.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
                baos.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return data;
    }

    @Override
    public Class<?> findClass(String className) throws ClassNotFoundException {
        System.out.println("findClass invoked: " + className);
        System.out.println("class loader name: " + classLoaderName);
        byte[] classBytes = this.loadClassData(className);
        return this.defineClass(className,classBytes,0,classBytes.length);
    }


    public static void test(ClassLoader classLoader) throws Exception {
        Class<?> clazz = classLoader.loadClass("com.song.cn.jvm.classloader.MyTest1");
        System.out.println("class: "+clazz.hashCode());
        Object object = clazz.newInstance();
        System.out.println("object:" +object);
        System.out.println("parent: "+object.getClass().getClassLoader());
    }

    public static void main(String[] args) throws Exception {
        MyTest18 loader1 = new MyTest18("loader1");
        loader1.setPath("/Users/sljoai/Desktop/Daily/");
        test(loader1);

        System.out.println("--------------------");

        // 将loader1 作为 loader2 的父加载器
        MyTest18 loader2 = new MyTest18(loader1,"loader2");
        loader2.setPath("/Users/sljoai/Desktop/Daily/");
        test(loader2);

        System.out.println("-----------------");
        // 父类为系统类加载器，而且loader1 和 loader3 属于不同的命名空间，所以需要重新加载MyTest1
        // 将loader1 作为 loader2 的父加载器
        MyTest18 loader3 = new MyTest18("loader3");
        loader3.setPath("/Users/sljoai/Desktop/Daily/");
        test(loader3);
        //findClass invoked: com.song.cn.jvm.classloader.MyTest1
        //class loader name: loader3
        //com/song/cn/jvm/classloader/MyTest1
        //class: 666988784
        //object:com.song.cn.jvm.classloader.MyTest1@5451c3a8
        //parent: [loader3]
    }
}
