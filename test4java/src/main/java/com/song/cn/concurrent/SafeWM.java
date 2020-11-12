package com.song.cn.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public class SafeWM {

    /**
     * 库存上限
     */
    private final AtomicInteger upper = new AtomicInteger(0);

    /**
     * 库存下限
     */
    private final AtomicInteger lower = new AtomicInteger(0);

    public void setUpper(int v){
        if(v<lower.get()){
            throw new IllegalArgumentException();
        }
        upper.set(v);
    }

    public void setLower(int v){
        if(v>upper.get()){
            throw new IllegalArgumentException();
        }
        lower.set(v);
    }

}
