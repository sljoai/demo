package com.song.cn.jvm.classloader;

public class MyTest6 {

    public static void main(String[] args) {
        Singleton singleton = Singleton.getInstance();
        System.out.println("counter1: "+Singleton.counter1);
        System.out.println("counter2: " + Singleton.counter2);
    }

}

class Singleton {
    public static int counter1;

    // 将该赋值语句移到 singleton 后面，按照加载顺序会被赋值为0
    public static int counter2 =0;

    private static Singleton singleton = new Singleton();

    //public static int counter2 =0;

    private Singleton() {
        counter1++;
        counter2++;// 在准备阶段会进行初始化
    }

    public static Singleton getInstance() {
        return singleton;
    }
}
