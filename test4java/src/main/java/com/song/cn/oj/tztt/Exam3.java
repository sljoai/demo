package com.song.cn.oj.tztt;

/**
 * 题目三：
 *
 * 1、现有一个JAVA工程Java_exam3（附件Java_exam3目录），工程直接引用了jar包datamanager_a.jar（历史依赖原因不可移除）。另从第三方得到两个jar包，datamanager_b.jar和datamanager_c.jar(均为java8编译)。三个jar里面都包含com.DataManager类,各实现好了一个String go(String param)方法：
 *
 * 	datamanager_a.jar的go方法用于向终端输出传入的字符串并返回解密串；
 *
 * 	datamanager_b.jar的go方法根据传入的String参数返回一个随机字符串；
 *
 * 	datamanager_c.jar的go方法可将传入的字符串进行加密后返回。
 *
 * 2、请使用该JAVA工程，先使用datamanager_b.jar生成随机字符串，再调用datamanager_c.jar将上一步返回的随机串进行加密，再调用datamanager_a.jar打印出加密字符串并返回解密串；
 *
 * 3、按文件Test.java类中test方法出入参数要求实现代码。
 *
 *
 *
 * 要求：不添加额外jar包，独立编程实现代码逻辑
 *
 * 考核知识点： 类加载, 反射
 */
public class Exam3 {
    public static void main(String[] args) {

    }
}
