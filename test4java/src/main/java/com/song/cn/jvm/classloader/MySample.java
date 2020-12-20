package com.song.cn.jvm.classloader;

public class MySample {
    public MySample(){
        System.out.println("MySample is loaded by: "+ this.getClass().getClassLoader());
        new MyCat();
        System.out.println("From MyCat: " + MyCat.class);
    }
}
