package com.song.cn.agent.spring.ioc.beans;

import com.song.cn.agent.spring.ioc.domain.User;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 使用静态方法创建Bean实例
 */
public class BeanInstantiationDemo {
    public static void main(String[] args) {
        // 配置 XML 配置文件
        // 启动 Spring 应用上下文
        BeanFactory beanFactory = new ClassPathXmlApplicationContext("classpath:/META-INF/bean-instantiation-context.xml");
        User user = beanFactory.getBean("user-by-static-method", User.class);
        System.out.println(user);

        User userByInstanceMethod = beanFactory.getBean("user-by-instance-method", User.class);
        System.out.println(userByInstanceMethod);

        User beanFactoryBean = beanFactory.getBean("user-by-factory-bean", User.class);
        System.out.println(beanFactoryBean);


    }
}
