package com.song.cn.jvm;

import org.openjdk.jol.info.ClassLayout;

public class Recorder {
    private byte a;
    private int b;
    private boolean c;
    private float d;
    private Object e;
    public static void main(String[] args) {
        System.out.println(
                ClassLayout
                        .parseClass(Recorder.class)
                        .toPrintable()
        );
    }
}
