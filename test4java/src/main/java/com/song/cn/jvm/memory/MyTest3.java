package com.song.cn.jvm.memory;

/**
 * 模拟死锁
 */
public class MyTest3 {
    public static void main(String[] args) {
        new Thread(()->A.method(),"Thread-A").start();
        new Thread(()->B.method(),"Thread-B").start();

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class A {
    public static synchronized void method(){
        System.out.println("method from A");
        try {
            Thread.sleep(1000);
            B.method();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class B {
    public static synchronized void method(){
        System.out.println("method from B");
        try {
            Thread.sleep(1000);
            A.method();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}




