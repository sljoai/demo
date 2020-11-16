package com.song.cn.jvm.memory;

import java.util.ArrayList;
import java.util.List;

/**
 * 复现内存异常的情况：
 * JVM_OPTIONS: -Xms5m -Xmx5m -XX:+HeapDumpOnOutOfMemoryError
 */
public class MyTest1 {
    public static void main(String[] args) {
        List<MyTest1> myTest1List = new ArrayList<>();
        for(;;){
            myTest1List.add(new MyTest1());

            System.gc();  // 显示调用垃圾回收，则不会出现OOM
        }
    }

}
