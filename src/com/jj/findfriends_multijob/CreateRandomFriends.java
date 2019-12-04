package com.jj.findfriends_multijob;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CreateRandomFriends {
    @Test
    public void test(){
        String outputPath = "H:\\bigdata-dev\\tools\\hadoop-data\\mr-findfriends-multijob\\input\\friends.txt";
        writeFriends(outputPath);
    }

    public void writeFriends(String outputPath){
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(outputPath);
            bw = new BufferedWriter(fw);
            Map<String, List<String>> map = createFriends('A', 26);
            for(String user : map.keySet()){
                List<String> friendsList = map.get(user);
                String friends = friendsList.toString().replace("[", "").replace("]", "").replace(" ","");
                bw.write(user+":"+friends);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(bw!=null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(fw!=null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Generate Random Friends data done!");
        }

    }


    public Map<String,List<String>> createFriends(char start,int size){
        char[] chs = new char[size];
        //构建用户组
        for(int i=0;i<size;i++){
            chs[i]=(char)(start+i);
        }
        //创建结果集
        Map<String, List<String>> resMap =new HashMap<>();
        Random random = new Random();
        //遍历用户组
        for(char c : chs){
            List<String> friendsList = new ArrayList<>();
            //随机生成每个用户好友数
            int amount = random.nextInt(size-1)+1;
            //构建当前用户好友
            for(int i=0;i<amount;i++){
                //标记好友是否已经添加正确
                boolean addedFlag = false;
                //随机生成好友
                char cr = (char)('A'+random.nextInt(size));
                //好友还未添加
                while(!addedFlag) {
                    //判断好友列表是否已有随机生成的好友，并且不是自己
                    if (!friendsList.contains(cr + "") && c != cr) {
                        friendsList.add(cr + "");
                        addedFlag = true;//正确添加并修正标记位
                    }else{
                        //否则重新生成好友
                        cr = (char)('A'+random.nextInt(size));
                    }
                }
            }
            //当前用户生成完添加进结果集
            resMap.put(c+"",friendsList);
        }
        return resMap;
    }
}
