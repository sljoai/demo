package com.song.cn.concurrent.semaphore;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

public class Test4Semaphore {
    public static void main(String[] args) {
        // 创建对象池
        ObjectPool<Long,String> pool = new ObjectPool(10,2L);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                // 通过对象池获取t,之后执行
                String ret = pool.exec(t -> {
                    System.out.println(t);
                    return Thread.currentThread().getName() + ": " +t.toString();
                });
                System.out.println(ret);
            }
        };
        Thread t1 = new Thread(task);
        t1.start();
        Thread t2 = new Thread(task);
        t2.start();
    }
}

class ObjectPool<T,R>{
    final List<T> pool;

    // 用信号量实现限流器
    final Semaphore sem;

    ObjectPool(int size,T t){
        // TODO: 此处是否可以使用ArrayList来代替呢？
        pool = new Vector<>();
        for(int i=0;i<size;i++){
            // TODO: 同一个对象被复制多份，并非严格意义上的对象池
            //pool.add(t);
            pool.add(t);
        }
        sem = new Semaphore(size);
    }

    /**
     * 利用对象池中的对象，调用func
     * @param function
     * @return
     */
    R exec(Function<T,R> function){
        T t = null;
        try {
            sem.acquire();
            t = pool.remove(0);
            // TODO: function的作用是什么呢？
            return function.apply(t);
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            pool.add(t);
            sem.release();
        }
        return null;
    }
}
