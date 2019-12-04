package com.jj.order_secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private String goodId;
    private double price;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeUTF(goodId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.goodId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + goodId + "\t" + price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //先按订单号升序
        int i = this.orderId - o.getOrderId();
        //订单号相同
        if (i == 0) {
            //按价格降序
            double j = o.getOrderId() - this.getPrice();
            //价格相同
            if (j == 0) {
                //按商品id升序
                return this.goodId.compareTo(o.getGoodId());
            }
            i = j > 0 ? 1 : -1;
        }
        return i;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getGoodId() {
        return goodId;
    }

    public void setGoodId(String goodId) {
        this.goodId = goodId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
