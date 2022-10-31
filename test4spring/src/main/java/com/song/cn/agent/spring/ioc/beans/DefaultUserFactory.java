package com.song.cn.agent.spring.ioc.beans;


import org.springframework.beans.factory.InitializingBean;

import javax.annotation.PostConstruct;

/**
 * User的默认实现
 */
public class DefaultUserFactory implements UserFactory, InitializingBean {


    @PostConstruct
    public void init() {
        System.out.println("@PostConstruct : UserFactory 初始化中...");
    }

    public void initUserFactory() {
        System.out.println("自定义初始化方法 initUserFactory() : UserFactory 初始化中...");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        System.out.println("InitializingBean#afterPropertiesSet() : UserFactory 初始化中...");
    }
}
