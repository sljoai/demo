package com.song.cn.jvm.classloader;


public class MyTest4 {

    public static void main(String[] args) {
        // 第一次主动使用该类时，才会访问 static 代码块
        MyParent4 myParent4 = new MyParent4();
        System.out.println("------");
        MyParent4 myParent41 = new MyParent4();

        MyParent4[] myParent4s = new MyParent4[1];
        System.out.println(myParent4s.getClass());
        System.out.println(myParent4s.getClass().getSuperclass());

        int[] ints = new int[1];
        System.out.println(ints.getClass());
        System.out.println(ints.getClass().getSuperclass());
    }
}

class MyParent4 {

    static {
        System.out.println("MyParent3 static code");
    }
}
