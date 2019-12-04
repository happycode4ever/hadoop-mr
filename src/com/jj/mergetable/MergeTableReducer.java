package com.jj.mergetable;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeTableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> beans = new ArrayList<>();
        Map<String,String> map = new HashMap<>();
        for(TableBean bean : values){
            //**迭代器迭代的对象地址一直是同一个,需要备份对象
            TableBean tableBean = new TableBean();
            String type = bean.getType();
            if("order".equals(type)){
                try {
                    BeanUtils.copyProperties(tableBean,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                beans.add(tableBean);
            }
            if("pd".equals(type)){
                map.put(bean.getpId(),bean.getProductName());
            }
        }

        for(TableBean bean : beans){
            bean.setProductName(map.get(bean.getpId()));
            context.write(bean,NullWritable.get());
        }
    }
}
