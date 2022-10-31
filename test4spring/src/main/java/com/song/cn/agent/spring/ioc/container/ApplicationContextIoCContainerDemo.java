package com.song.cn.agent.spring.ioc.container;

import com.song.cn.agent.spring.ioc.domain.User;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Map;

/**
 *
 */
public class ApplicationContextIoCContainerDemo {
    public static void main(String[] args) {
        //创建 BeanFactory 容器
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();

        //将当前类（ApplicationContextIoCContainerDemo）作为配置类（Configuration Class）
        applicationContext.register(ApplicationContextIoCContainerDemo.class);

        //启动应用上下文
        applicationContext.refresh();

        //依赖查找集合对象
        lookupByCollectionType(applicationContext);

    }

    /**
     * 通过 Java注解的方式，定义一个Bean
     *
     * @return
     */
    @Bean
    public User getUser() {
        User user = new User();
        user.setId(1);
        user.setName("小马哥");
        return user;
    }

    private static void lookupByCollectionType(BeanFactory beanFactory) {
        if (beanFactory instanceof ListableBeanFactory) {
            ListableBeanFactory listableBeanFactory = (ListableBeanFactory) beanFactory;
            Map<String, User> users = listableBeanFactory.getBeansOfType(User.class);
            System.out.println("查找所有的User对象：" + users);
        }
    }
}
