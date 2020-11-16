package com.song.cn.jvm.bytecode;

/**
 * 静态分派
 */
public class MyTest5 {

    public void test(Grandpa grandpa){
        System.out.println("grandpa");
    }
    public void test(Father father){
        System.out.println("father");
    }
    public void test(Son son){
        System.out.println("son");
    }

    public static void main(String[] args) {
        Grandpa p1 = new Father();
        Grandpa p2 = new Son();

        MyTest5 myTest5 = new MyTest5();
        myTest5.test(p1); // grandpa
        myTest5.test(p2); // grandpa
    }
}

class Grandpa{

}

class Father extends Grandpa{

}

class Son extends Father{

}
