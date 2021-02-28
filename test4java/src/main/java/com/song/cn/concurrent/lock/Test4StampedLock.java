package com.song.cn.concurrent.lock;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;


public class Test4StampedLock {
    final StampedLock  stampedLock = new StampedLock();

    /**
     * 验证线程在 StampedLock#readLock或writeLock后 再调用interrupt方法，会导致CPU飙升
     */
    @Test
    public  void test4Interrupt() throws InterruptedException {
        Thread t1 = new Thread(()->{
            // 获取写锁
            stampedLock.writeLock();
            // 永远阻塞在这里，不释放写锁
            LockSupport.park();
        });
        t1.start();
        // 保证t1获取写锁
        Thread.sleep(100);
        Thread t2 = new Thread(()->{
            // 阻塞在悲观读锁
            stampedLock.readLock();
        });
        t2.start();
        // 保证t2阻塞在读锁
        Thread.sleep(100);
        // 终端线程t2
        // 会导致线程t2所在CPU飙升
        t2.interrupt();
        t2.join();
    }
    @Test
    public void readTemplate(){
        long stamp = stampedLock.tryOptimisticRead();
        // 读写方法局部变量
        // TODO
        // 校验stamp
        if(!stampedLock.validate(stamp)){
            // 升级为悲观读锁
            stamp = stampedLock.readLock();
            try {
                //读入方法局部变量
            }finally {
                // 释放悲观读锁
                stampedLock.unlock(stamp);
            }
        }
        // TODO: 完成业务逻辑处理
    }

    @Test
    public void writeTemplate(){
        long stamp = stampedLock.writeLock();
        try {
            // 写共享变量
            // TODO
        }finally {
            stampedLock.unlock(stamp);
        }
    }

}
