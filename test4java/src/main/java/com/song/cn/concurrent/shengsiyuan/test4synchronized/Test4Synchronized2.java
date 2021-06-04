package com.song.cn.concurrent.shengsiyuan.test4synchronized;

/**
 * 需求一：编写一个多线程程序，实现以下目标
 * 1. 存在一个对象，该对象有一个 int 类型的成员变量，该成员变量初始值为0
 * 2. 创建四个线程，两个用于增加成员变量，两个用于减小成员变量
 * 3. 输出该对象成员变量每次变化后的值
 * 4. 最终输出的结果为 101010...
 */
public class Test4Synchronized2 {
    public static void main(String[] args) {
        MyObject2 myObject = new MyObject2();

        IncreaseThread2 increase = new IncreaseThread2(myObject);
        Thread increaseThread = new Thread(increase);
        Thread increaseThread2 = new Thread(increase);

        DecreaseThread2 decrease = new DecreaseThread2(myObject);
        Thread decreaseThread = new Thread(decrease);
        Thread decreaseThread2 = new Thread(decrease);

        // 启动两个递减线程
        decreaseThread.start();
        decreaseThread2.start();

        // 启动两个递增线程
        increaseThread.start();
        increaseThread2.start();

    }
}

class DecreaseThread2 implements Runnable {
    private MyObject2 myObject;

    public DecreaseThread2(MyObject2 myObject) {
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
class IncreaseThread2 implements Runnable {

    private MyObject2 myObject;

    public IncreaseThread2(MyObject2 myObject) {
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

class MyObject2 {
    /**
     * 记录出现次数
     */
    private int counters;

    public synchronized void increase() throws InterruptedException {
        while (counters != 0) {
            wait();
        }
        counters++;
        System.out.println("increase and counter: " + counters);
        notify();
    }

    public synchronized void decrease() throws InterruptedException {
        while (counters == 0) {
            wait();
        }
        counters--;
        System.out.println("decrease and counter: " + counters);
        notify();
    }
}
