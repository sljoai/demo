package com.song.cn.jvm.bytecode;

/**
 * 栈帧（Stack Frame）
 */
public class MyTest4 {
    public static void test(){
        System.out.println("test invoked.");
    }
    public static void main(String[] args) {
        test();
    }
}
