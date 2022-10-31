package com.song.cn.agent.spring.ioc.beans;

import com.song.cn.agent.spring.ioc.domain.User;

/**
 * User 工厂类
 */
public interface UserFactory {
    //提供默认实现
    default User createUser() {
        return User.createUser();
    }
}
