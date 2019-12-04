package com.jj.order_secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {
    public OrderGroupComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        //同一个订单号的bean的key的数据都分组到一个reduce
        return o1.getOrderId() - o2.getOrderId();

    }
}
