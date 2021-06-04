package com.song.cn.flink.nx.state.order;

import org.apache.commons.lang3.StringUtils;

public class OrderInfo1 {
    /**
     * 订单ID
     */
    private String orderId;

    /**
     * 商品名称
     */
    private String itemName;

    /**
     * 商品价格
     */
    private double price;

    public OrderInfo1() {
    }

    public OrderInfo1(String orderId, String itemName, double price) {
        this.orderId = orderId;
        this.itemName = itemName;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderInfo1{" +
                "orderId='" + orderId + '\'' +
                ", itemName='" + itemName + '\'' +
                ", price=" + price +
                '}';
    }

    public static OrderInfo1 str2OrderInfo1(String line){
        OrderInfo1 orderInfo1 = new OrderInfo1();

        if(StringUtils.isNoneBlank(line)){
            String[] split = line.split(",");
            orderInfo1.setOrderId(split[0]);
            orderInfo1.setItemName(split[1]);
            orderInfo1.setPrice(Double.valueOf(split[2]));
        }
        return orderInfo1;
    }
}
