package com.song.cn.concurrent.shengsiyuan.test4synchronized;

/**
 * 需求一：编写一个多线程程序，实现以下目标
 *  1. 存在一个对象，该对象有一个 int 类型的成员变量，该成员变量初始值为0
 *  2. 创建两个线程，一个线程对该对象的成员变量增1，另一个线程对该对象的成员变量减1
 *  3. 输出该对象成员变量每次变化后的值
 *  4. 最终输出的结果为 101010...
 *
 *  需求二：
 *  2. 创建四个线程，两个用于增加成员变量，两个用于减小成员变量
 */
public class Test4Synchronized {
    public static void main(String[] args) {
        MyObject myObject = new MyObject();
        Thread increaseThread = new Thread(new IncreaseThread(myObject));
        Thread decreaseThread = new Thread(new DecreaseThread(myObject));
        increaseThread.start();
        decreaseThread.start();
    }
}

class DecreaseThread implements Runnable {
    private MyObject myObject;

    public DecreaseThread(MyObject myObject) {
        this.myObject = myObject;
    }

    @Override
    public void run() {
        for (int i = 0; i < 30; i++) {
            try {
                Thread.sleep(1000);
                myObject.decrease();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

/**
 * 递增线程
 */
class IncreaseThread implements Runnable {

    private MyObject myObject;

    public IncreaseThread(MyObject myObject) {
        this.myObject = myObject;
    }

    @Override
    public void run() {
        for (int i = 0; i < 30; i++) {
            try {
                Thread.sleep(1000);
                myObject.increase();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class MyObject {
    /**
     * 记录出现次数
     */
    private int counters;

    public synchronized void increase() throws InterruptedException {
        if (counters != 0) {
            wait();
        }
        counters++;
        System.out.println("increase and counter: " + counters);
        notify();
    }

    public synchronized void decrease() throws InterruptedException {
        if (counters == 0) {
            wait();
        }
        counters--;
        System.out.println("decrease and counter: " + counters);
        notify();
    }
}
