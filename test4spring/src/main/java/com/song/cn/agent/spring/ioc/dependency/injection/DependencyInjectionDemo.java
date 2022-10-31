package com.song.cn.agent.spring.ioc.dependency.injection;

import com.song.cn.agent.spring.ioc.repository.UserRepository;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 依赖注入示例：
 * 1，根据名称进行查找
 */
public class DependencyInjectionDemo {

    public static void main(String[] args) {
        //配置XML文件
        //启动 Spring 上下文
        BeanFactory beanFactory = new ClassPathXmlApplicationContext("classpath:/META-INF/dependency-injection-context.xml");

        //依赖注入：自定义bean（首先是依赖查找，然后是依赖注入）
        UserRepository userRepository = (UserRepository) beanFactory.getBean("userRepository");
        System.out.println(userRepository.getUsers());

        //依赖注入：内建依赖
        System.out.println(userRepository.getBeanFactory());

        ObjectFactory<ApplicationContext> userFactory = userRepository.getObjectFactory();

        System.out.println(userFactory.getObject() == beanFactory);

    }

}
