package com.song.cn.jvm.bytecode;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * 解析动态代理对象生成过程
 */
public class Client {
    public static void main(String[] args) {
        // 将代理生成的Class文件保存到本地
        System.getProperties().setProperty("sun.misc.ProxyGenerator.saveGeneratedFiles","true");
        RealSubject rs = new RealSubject();
        InvocationHandler ds = new DynamicSubject(rs);
        Class<?> cls = rs.getClass();

        Subject subject = (Subject) Proxy.newProxyInstance(cls.getClassLoader(),
                cls.getInterfaces(),
                ds);

        subject.request();
        System.out.println(subject.getClass());
        System.out.println(subject.getClass().getSuperclass());
    }
}
