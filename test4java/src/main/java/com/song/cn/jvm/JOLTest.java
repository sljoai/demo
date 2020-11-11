package com.song.cn.jvm;

import org.openjdk.jol.vm.VM;

public class JOLTest {
    public static void main(String[] args) {
        System.out.println(VM.current().details());
    }
}
