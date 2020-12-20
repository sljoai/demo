package com.song.cn.jvm.classloader;

import java.io.*;

public class MyTest19  extends ClassLoader{
    private String classLoaderName;

    private final String fileExtention = ".class";

    private String path;

    public MyTest19(String classLoaderName) {
        super(); // 将系统类加载器当作该类加载器的父加载器
        this.classLoaderName = classLoaderName;
    }

    public MyTest19(ClassLoader parent, String classLoaderName) {
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

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        MyTest19 myTest19 = new MyTest19("loader1");
        myTest19.setPath("/Users/sljoai/Desktop/Daily/");
        Class<?> clazz = myTest19.loadClass("com.song.cn.jvm.classloader.MyTest1");
        System.out.println("class: "+clazz.hashCode());
        Object object = clazz.newInstance();
        System.out.println("object:" +object);
        System.out.println("parent: "+object.getClass().getClassLoader());

        // 显示将 实例对象 、class对象、类加载器置为null
        object = null;
        clazz = null;
        myTest19 = null;

        // JVMOption: -XX:+TraceClassUnloading
        System.gc();
        System.out.println("----------------------");

        myTest19 = new MyTest19("loader2");
        myTest19.setPath("/Users/sljoai/Desktop/Daily/");
        clazz = myTest19.loadClass("com.song.cn.jvm.classloader.MyTest1");
        System.out.println("class: "+clazz.hashCode());
        object = clazz.newInstance();
        System.out.println("object:" +object);
        System.out.println("parent: "+object.getClass().getClassLoader());
    }
}
