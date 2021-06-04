package com.song.cn.flink.nx.state.order;

import org.apache.commons.lang3.StringUtils;

public class OrderInfo2 {
    /**
     * 订单ID
     */
    private String orderId;

    /**
     * 下单时间
     */
    private String orderTime;

    /**
     * 下单省份
     */
    private String province;

    public OrderInfo2() {
    }

    public OrderInfo2(String orderId, String orderTime, String province) {
        this.orderId = orderId;
        this.orderTime = orderTime;
        this.province = province;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "OrderInfo2{" +
                "orderId='" + orderId + '\'' +
                ", orderTime='" + orderTime + '\'' +
                ", province='" + province + '\'' +
                '}';
    }

    public static OrderInfo2 str2OrderInfo2(String line){
        OrderInfo2 orderInfo2 = new OrderInfo2();

        if(StringUtils.isNoneBlank(line)){
            String[] split = line.split(",");
            orderInfo2.setOrderId(split[0]);
            orderInfo2.setOrderTime(split[1]);
            orderInfo2.setProvince(split[2]);
        }
        return orderInfo2;
    }
}
