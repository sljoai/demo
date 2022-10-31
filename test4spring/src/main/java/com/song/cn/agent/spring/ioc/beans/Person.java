package com.song.cn.agent.spring.ioc.beans;

/**
 * 描述人的 POJO 类
 * <p>
 * Setter / Getter 方法
 * 可写方法（Writable） / 可读方法（Readable）
 * </p>
 */
public class Person {

    String name;

    String age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
}
