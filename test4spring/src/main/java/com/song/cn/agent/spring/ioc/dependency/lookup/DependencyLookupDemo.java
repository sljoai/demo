package com.song.cn.agent.spring.ioc.dependency.lookup;

import com.song.cn.agent.spring.ioc.annotation.Super;
import com.song.cn.agent.spring.ioc.domain.User;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

/**
 * 依赖查找示例：
 * 1，根据名称进行查找
 * 2，根据类型进行查找
 */
public class DependencyLookupDemo {

    public static void main(String[] args) {
        //配置XML文件
        //启动 Spring 上下文
        BeanFactory beanFactory = new ClassPathXmlApplicationContext("classpath:META-INF/dependency-lookup-context.xml");
        lookupInRealTime(beanFactory);
        lookupInLazy(beanFactory);

        lookupByType(beanFactory);
        lookupByCollectionType(beanFactory);

        lookupByAnnotation(beanFactory);
    }

    private static void lookupByAnnotation(BeanFactory beanFactory) {
        if (beanFactory instanceof BeanFactory) {
            ListableBeanFactory listableBeanFactory = (ListableBeanFactory) beanFactory;
            Map<String, User> users = (Map) listableBeanFactory.getBeansWithAnnotation(Super.class);
            System.out.println("查找所有 @Super 的User对象：" + users);
        }
    }

    private static void lookupByCollectionType(BeanFactory beanFactory) {
        if (beanFactory instanceof ListableBeanFactory) {
            ListableBeanFactory listableBeanFactory = (ListableBeanFactory) beanFactory;
            Map<String, User> users = listableBeanFactory.getBeansOfType(User.class);
            System.out.println("查找所有的User对象：" + users);
        }
    }

    private static void lookupByType(BeanFactory beanFactory) {
        User user = beanFactory.getBean(User.class);
        System.out.println("类型查找：" + user);
    }

    public static void lookupInRealTime(BeanFactory beanFactory) {
        User user = (User) beanFactory.getBean("user");
        System.out.println("实时查找：" + user);
    }

    public static void lookupInLazy(BeanFactory beanFactory) {
        ObjectFactory<User> factoryBean = (ObjectFactory<User>) beanFactory.getBean("objectFactory");
        User user = factoryBean.getObject();
        System.out.println("延迟查找：" + user);
    }
}
