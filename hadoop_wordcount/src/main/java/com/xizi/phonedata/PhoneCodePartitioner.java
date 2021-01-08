package com.xizi.phonedata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class PhoneCodePartitioner extends Partitioner<Text,PhoneWritable> {

    static Map<String,Integer> map=new HashMap<>();
    static {
        map.put("137",0);
        map.put("138",1);
        map.put("139",2);
    }

    @Override
    public int getPartition(Text key, PhoneWritable phoneWritable, int numPartitions) {
        //手机号的前三位
        String phonePrefix = key.toString().substring(0, 3);

        return map.get(phonePrefix)==null?3:map.get(phonePrefix);
    }
}
