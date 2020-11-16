package com.song.cn.jvm.bytecode;

public class RealSubject implements Subject{
    @Override
    public void request() {
        System.out.println("From Real Subject");
    }
}
