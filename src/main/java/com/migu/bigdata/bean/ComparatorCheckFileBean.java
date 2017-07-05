package com.migu.bigdata.bean;

import java.util.Comparator;
public class ComparatorCheckFileBean implements Comparator<CheckFileBean> {
    @Override
    public int compare(CheckFileBean u1, CheckFileBean u2){
        int flag = u1.getAccessTime().compareTo(u2.getAccessTime());
        if (flag == 0) {
            return u1.getDataTime().compareTo(u2.getDataTime());
        } else {
            return flag;
        }
    }
}
