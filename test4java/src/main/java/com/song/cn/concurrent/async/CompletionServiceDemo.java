package com.song.cn.concurrent.async;

import java.util.concurrent.*;

public class CompletionServiceDemo {
    public static void main(String[] args) {

    }

    public static void test3() {
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // 异步向电商s1询价
        Future<Integer> f1 = executor.submit(() -> getPriceByS1());
        // 异步向电商s2询价
        Future<Integer> f2 = executor.submit(() -> getPriceByS2());
        // 异步向电商s3询价
        Future<Integer> f3 = executor.submit(() -> getPriceByS3());

        // 创建阻塞队列
        LinkedBlockingQueue<Integer> bq = new LinkedBlockingQueue<>();
    }

    /**
     * 异步询价，异步保存
     */
    public static void test2(){
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // 异步向电商s1询价
        Future<Integer> f1 = executor.submit(() -> getPriceByS1());
        // 异步向电商s2询价
        Future<Integer> f2 = executor.submit(() -> getPriceByS2());
        // 异步向电商s3询价
        Future<Integer> f3 = executor.submit(() -> getPriceByS3());

        // 创建阻塞队列
        LinkedBlockingQueue<Integer> bq = new LinkedBlockingQueue<>();

        // 电商s1报价异步进入阻塞队列
        executor.execute(() -> {
            try {
                bq.put(f1.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        // 电商s2报价异步进入阻塞队列
        executor.execute(() -> {
            try {
                bq.put(f2.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        // 电商s3报价异步进入阻塞队列
        executor.execute(() -> {
            try {
                bq.put(f3.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        // 异步保存所有报价
        for(int i=0;i<3;i++){
            try {
                Integer r = bq.take();
                executor.execute(() -> save(r));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 异步获取报价，同步保存报价<br>
     *     Future.get 会阻塞主线程
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void test1() throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // 异步向电商s1询价
        Future<Integer> f1 = executor.submit(() -> getPriceByS1());
        // 异步向电商s2询价
        Future<Integer> f2 = executor.submit(() -> getPriceByS2());
        // 异步向电商s3询价
        Future<Integer> f3 = executor.submit(() -> getPriceByS3());

        // 获取电商s1报价并保存
        Integer r1 = f1.get();
        executor.execute(() -> save(r1) );

        // 获取电商s2报价并保存
        Integer r2 = f2.get();
        executor.execute(() -> save(r2) );

        // 获取电商s3报价并保存
        Integer r3 = f3.get();
        executor.execute(() -> save(r3) );
        System.out.printf("cost time -> %d \r\r",System.currentTimeMillis()-start);
    }

    private static Integer getPriceByS1(){
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 1;
    }

    private static Integer getPriceByS2(){
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 2;
    }

    private static Integer getPriceByS3(){
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 3;
    }

    public static void save(Integer r){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.printf("r -> %d  \r\n",r);
    }
}
