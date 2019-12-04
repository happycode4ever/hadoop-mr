package com.jj.phonedata;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneDataBean implements Writable {
    //可以保留不需要mr传输的属性无须序列化
    private int id;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public PhoneDataBean() {
    }

    public PhoneDataBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        //单个手机号流量汇总
        this.sumFlow=upFlow+downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return this.upFlow+"\t"+this.downFlow+"\t"+this.sumFlow;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }
    //单个手机号流量汇总设置
    public void setSumFlow(long sumFlow) {
        this.sumFlow = this.upFlow+this.downFlow;
    }
}
