package com.jj.test;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IteratorTest {

    @Test
    public void test(){
        List<String> l1 = new ArrayList<>();
        l1.add("aaaa");
        l1.add("bbbb");
        l1.add("cccc");
        l1.add("dddd");
        List<String> l2 = new ArrayList<>();
        Iterable<String> it = l1;
        for(String s : it){
            System.out.println(s);
            l2.add(s);
        }
        System.out.println(l2);
    }
}
