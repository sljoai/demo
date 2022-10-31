package com.song.cn.agent.spring.ioc.beans;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyEditorSupport;
import java.util.stream.Stream;

public class BeanInfoDemo {

    public static void main(String[] args) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(Person.class, Object.class);

        Stream.of(beanInfo.getPropertyDescriptors()).forEach(propertyDescriptor -> {
                    //PropertyDescriptor允许添加属性边界其 PropertyEditor

                    Class<?> propertyType = propertyDescriptor.getPropertyType();
                    if ("age".equals(propertyType)) { // 为 “age” 字段或属性添加 PropertyEditor字段
                        //String -> Integer
                        propertyDescriptor.setPropertyEditorClass(String2IntegerPropertyEditor.class);
                    }
                }
        );

    }

    static class String2IntegerPropertyEditor extends PropertyEditorSupport {
        @Override
        public void setAsText(String text) throws IllegalArgumentException {
            Integer value = Integer.valueOf(text);
            setValue(value);
        }
    }
}
