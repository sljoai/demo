package com.song.cn.concurrent.java8;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
public class ParallelTest {

    @Test
    public void parallel() {
        IntStream.rangeClosed(1, 100).parallel().forEach(i -> {
            System.out.println(LocalDateTime.now() + " : " + i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        });
    }

    @Test
    public void allMethods() throws InterruptedException, ExecutionException {
        int taskCount = 10000;
        int threadCount = 20;
        StopWatch stopWatch = new StopWatch();

        stopWatch.start("thread");
        Assert.assertEquals(taskCount, thread(taskCount, threadCount));
        stopWatch.stop();

        stopWatch.start("threadpool");
        Assert.assertEquals(taskCount, threadpool(taskCount, threadCount));
        stopWatch.stop();

        //试试把这段放到forkjoin下面？
        stopWatch.start("stream");
        Assert.assertEquals(taskCount, stream(taskCount, threadCount));
        stopWatch.stop();

        stopWatch.start("forkjoin");
        Assert.assertEquals(taskCount, forkjoin(taskCount, threadCount));
        stopWatch.stop();

        stopWatch.start("completableFuture");
        Assert.assertEquals(taskCount, completableFuture(taskCount, threadCount));
        stopWatch.stop();

        log.info(stopWatch.prettyPrint());
    }

    private void increment(AtomicInteger atomicInteger) {
        atomicInteger.incrementAndGet();
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int thread(int taskCount, int threadCount) throws InterruptedException {
        // 总操作次数计数器
        AtomicInteger atomicInteger = new AtomicInteger();
        // 使用CountDownLatch 来等待所有线程执行完成
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        // 使用IntStream 把数字直接转为Thread
        IntStream.rangeClosed(1, threadCount).mapToObj(i -> new Thread(() -> {
            // 手动把taskCount分成taskCount份，每份有一个线程执行
            IntStream.rangeClosed(1, taskCount / threadCount).forEach(j -> increment(atomicInteger));
            // 每一个线程处理完成自己那部分数据之后，countDown一次
            countDownLatch.countDown();
        })).forEach(Thread::start);
        // 等到所有线程执行完成
        countDownLatch.await();
        // 查询计数器当前值
        return atomicInteger.get();
    }

    private int threadpool(int taskCount, int threadCount) throws InterruptedException {
        // 总操作数计数器
        AtomicInteger atomicInteger = new AtomicInteger();
        // 初始化一个线程数量=threadCount的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        // 所有任务直接提交到线程池中处理
        IntStream.rangeClosed(1, taskCount).forEach(i ->
                executorService.execute(() -> increment(atomicInteger)));
        // 提交关闭线程池申请，等待之前所有任务执行完成
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        // 查询计数器当前值
        return atomicInteger.get();
    }

    private int forkjoin(int taskCount, int threadCount) throws InterruptedException {
        // 总操作次数计数器
        AtomicInteger atomicInteger = new AtomicInteger();
        // 自定义一个并行度=threadCount的ForkJoinPool
        ForkJoinPool forkJoinPool = new ForkJoinPool(threadCount);
        // 所有任务直接提交到线程池处理
        forkJoinPool.execute(() -> IntStream.rangeClosed(1, taskCount).parallel().forEach(i -> increment(atomicInteger)));
        // 提交关闭线程池申请，等待之前所有任务执行完成
        forkJoinPool.shutdown();
        forkJoinPool.awaitTermination(1, TimeUnit.HOURS);
        // 查询计数器当前值
        return atomicInteger.get();
    }

    private int stream(int taskCount, int threadCount) {
        // 设置公共ForkJoinPool的并行度
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(threadCount));
        // 总操作次数计数器
        AtomicInteger atomicInteger = new AtomicInteger();
        // 由于我们设置了公共ForkJoinPool并行度，直接使用parallel提交任务
        IntStream.rangeClosed(1, taskCount).parallel().forEach(i -> increment(atomicInteger));
        return atomicInteger.get();
    }

    private int completableFuture(int taskCount, int threadCount) throws InterruptedException, ExecutionException {
        AtomicInteger atomicInteger = new AtomicInteger();
        ForkJoinPool forkJoinPool = new ForkJoinPool(threadCount);
        CompletableFuture.runAsync(() ->
                IntStream.rangeClosed(1, taskCount)
                        .parallel()
                        .forEach(i -> increment(atomicInteger)), forkJoinPool)
                .get();
        return atomicInteger.get();
    }
}
