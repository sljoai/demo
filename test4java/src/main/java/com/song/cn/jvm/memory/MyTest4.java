package com.song.cn.jvm.memory;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

/**
 * 方法区产生内存溢出错误
 */
public class MyTest4 {
    public static void main(String[] args) {
        // 不断创建子类
        for(;;){
            Enhancer enhancer = new Enhancer();
            // 设置父类
            enhancer.setSuperclass(MyTest4.class);
            enhancer.setUseCache(false);
            enhancer.setCallback((MethodInterceptor)(obj, method, args1, proxy) ->
                    proxy.invokeSuper(obj,args1));
            System.out.println("Hello World");
            // 创建 MyTest4的子类
            enhancer.create();
        }
    }
}
