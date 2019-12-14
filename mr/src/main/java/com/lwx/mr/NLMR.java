package com.lwx.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NLMR {

    public static class NLMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

    }

    public static class NLReducer extends Reducer<Text,IntWritable,Text,Text>{

    }

    public static void main(String[] args) {

    }
}
