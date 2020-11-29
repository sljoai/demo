package com.song.cn.jvm.classloader;

public class MyTest12 {
    static {
        System.out.println("MyTest12 static block.");
    }

    public static void main(String[] args) {
        System.out.println(Child3.a); // 对 Parent3的主动使用,所以不会打印Child3中的静态代码块
        System.out.println("----------------");
        Child3.doSomething();
    }
}

class Parent3{
    static int a =3;
    static {
        System.out.println("Parent3 static block.");
    }

    static void doSomething(){
        System.out.println("do something");
    }
}

class Child3 extends Parent3{
    static int b =4;
    static {
        System.out.println("Child3 static block.");
    }
}
