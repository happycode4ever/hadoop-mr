package com.jj.mergetable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    //1001	01	1
    //1002	02	2
    //01	小米
    //02	华为
    private String orderId="";
    private String pId="";
    private int amount;
    private String productName="";
    private String type="";

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeInt(amount);
        out.writeUTF(productName);
        out.writeUTF(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pId = in.readUTF();
        this.amount = in.readInt();
        this.productName = in.readUTF();
        this.type = in.readUTF();
    }

    @Override
    public String toString() {
        return orderId+"\t"+pId+"\t"+productName+"\t"+amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
