package com.song.cn.concurrent.atomic;

import java.util.concurrent.atomic.AtomicReference;

public class SafeWMSeveral {

    final AtomicReference<WMRange> rf = new AtomicReference<>(new WMRange(0,0));

    public void upper(int v) {
        WMRange nr;
        WMRange or =rf.get();
        do{
            if(v<or.lower){
                throw new IllegalArgumentException();
            }
            nr = new WMRange(v,or.lower);
        }while(!rf.compareAndSet(or,nr));
    }

    class WMRange {
        final int upper;
        final int lower;
        WMRange(int upper,int lower){
            this.upper = upper;
            this.lower = lower;
        }
    }

}
