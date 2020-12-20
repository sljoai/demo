package com.song.cn.jvm.classloader;

import java.io.*;

public class MyTest16 extends ClassLoader {

    private String classLoaderName;

    private final String fileExtension = ".class";

    public MyTest16(ClassLoader parent){
        super(parent);
    }
    public MyTest16(String classLoaderName) {
        super(); // 将系统类加载器当作该类加载器的父加载器
        this.classLoaderName = classLoaderName;
    }

    public MyTest16(ClassLoader parent, String classLoaderName) {
        super(parent); // 显示指定该类加载器的父加载器
        this.classLoaderName = classLoaderName;
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
        InputStream is = null;
        byte[] data = null;
        ByteArrayOutputStream baos = null;

        try {
            //TODO: 未调用
            String clazzFileName = fullClassName.replace(".", "/");
            is = new FileInputStream(new File(clazzFileName + this.fileExtension));
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
        Object object = clazz.newInstance();
        System.out.println(object);
        System.out.println(object.getClass().getClassLoader());
    }

    public static void main(String[] args) throws Exception {
        MyTest16 loader1 = new MyTest16("loader1");
        test(loader1);
    }
}
