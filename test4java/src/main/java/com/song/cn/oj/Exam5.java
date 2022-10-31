package com.song.cn.oj;

import java.util.Random;

public class Exam5 {
    public static void main(String[] args) {
        Items items = new Items();
        Thread seller = new Thread(new Seller(items));
        Thread buyer = new Thread(new Buyer(items));
        seller.start();
        buyer.start();
    }
}

class Items {
    private int num = 0;

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

/**
 * 销售业务逻辑
 */
class Seller implements Runnable {

    /**
     * 两次销售间隔时间（ms）
     */
    private final int MAX_WAIT_TIME = 2000;
    private long count = 0;
    /**
     * 记录商品销售信息
     */
    private Items items;

    /**
     * 用于生成两次销售活动等待时间随机数
     */
    private Random random;

    public Seller(Items items) {
        this.items = items;
        this.random = new Random();
    }

    @Override
    public void run() {
        while (true) {
            // 随机生成执行时间
            synchronized (items) {
                while (items.getNum() <= 0) {
                    try {
                        items.wait();
                    } catch (InterruptedException e) {
                        System.err.println("");
                    }
                }
                sell();
                // 当发现商品数量小于10，提醒进货
                if (items.getNum() < 10) {
                    items.notify();
                }
            }
            // 随机生成等待时间
            int waitTime = random.nextInt(MAX_WAIT_TIME);
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                System.err.println("当前销售等待出错！");
            }
        }
    }

    /**
     * 执行销售业务逻辑
     */
    private void sell() {
        int before = items.getNum();
        // 随机生成销售数量
        int reduce = random.nextInt(before);
        int after = before - reduce;
        items.setNum(after);
        System.out.printf("第 [ %s ] 次销售 - 本次销售数量: [ %s], 销售之后总数量: [ %s ] \n", ++count, reduce, after);
    }
}

class Buyer implements Runnable {
    Items items;
    Random random;
    long count = 0;

    public Buyer(Items items) {
        this.items = items;
        this.random = new Random();
    }


    @Override
    public void run() {
        while (true) {
            // 随机生成执行时间
            synchronized (items) {
                while (items.getNum() >= Common.TEN) {
                    try {
                        items.wait();
                    } catch (InterruptedException e) {
                        System.err.println("");
                    }
                }
                buy();
                // 当发现商品数量小于10，提醒进货
                items.notify();
            }
        }
    }

    /**
     * 补货
     */
    private void buy() {
        int before = items.getNum();
        int add = random.nextInt(Common.FIFTY) + Common.FIFTY;
        int after = before + add;
        items.setNum(after);
        System.out.printf("第 [ %s ] 次进货 - 本次进货数量: [ %s], 进货之后总数量: [ %s ] \n", ++count, add, after);
    }
}

class Common {
    public static int TEN = 10;
    public static int FIFTY = 50;
}
