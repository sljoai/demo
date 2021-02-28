package com.song.cn.concurrent.atomic;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;

public class SimulatedCAS {

    volatile int count;
    /**
     * 模拟实现 CAS，仅用来帮助理解
     * @param expect
     * @param newValue
     * @return
     */
    private synchronized int cas(int expect,int newValue){
        // 读目前count的值
        int curValue = count;
        // 比较目前count值是否==期望值
        if(curValue == expect){
            // 如果是，则更新count的值
            count = newValue;
        }
        return curValue;
    }

    private void addOne(){
        int newValue = 0;
        do {
            newValue = count +1;
        }while (count !=cas(count,newValue));
    }

    @Before
    public void before(){
        count = 0;
    }

    @Test
    public void test4CAS(){
        ForkJoinPool forkJoinPool = new ForkJoinPool(10);
        forkJoinPool.execute(()->{
            addOne();
            System.out.println(count);
        });

    }
}
