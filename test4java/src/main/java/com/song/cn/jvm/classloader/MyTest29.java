package com.song.cn.jvm.classloader;

public class MyTest29 implements Runnable{
    private Thread thread;

    public MyTest29(){
        thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        ClassLoader classLoader = this.thread.getContextClassLoader();
        this.thread.setContextClassLoader(classLoader);
        System.out.println("Class: "+ classLoader.getClass());
        System.out.println("Parent: "+ classLoader.getParent().getClass());
    }

    public static void main(String[] args) {
        new MyTest29();
    }
}
