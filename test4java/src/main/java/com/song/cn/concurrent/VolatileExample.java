package com.song.cn.concurrent;

public class VolatileExample {
    int x = 0;
    volatile boolean v = false;

    public void writer() {
        x = 42;
        v = true;
    }

    public void reader() {
        if (v == true) {
            // 这里 x 会是多少？
            // 对于 jdk 1.5  之前，x 可能是0，也可能是42；jdk 1.5 后，x只能是42
        }
    }
}
