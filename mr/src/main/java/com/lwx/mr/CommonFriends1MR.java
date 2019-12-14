package com.lwx.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//A:B,C,D,F,E,O
//B:A,C,E,K
//C:F,A,D,I
//D:A,E,F,L
//E:B,C,D,M,L
//F:A,B,C,D,E,O,M
//G:A,C,D,E,F
//H:A,C,D,E,O
//I:A,O
//J:B,O
//K:A,C,D
//L:D,E,F
//M:E,F,G
//O:A,H,I,J,K


public class CommonFriends1MR {

    public static class CFMapper extends Mapper<LongWritable,Text, Text,Text>{
        Text k=new Text();
        Text v=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] data=line.split(":");

            String person=data[0];
            String[] friends=data[1].split(",");

            for(String friend:friends){
                k.set(friend);
                v.set(person);
                context.write(k,v);
            }


        }
    }


    public static class CFReducer extends Reducer<Text,Text,Text,Text>{
        Text v=new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer("");
            for(Text p:values){
                sb.append(p).append("-");
            }
            v.set(sb.toString());
            context.write(key,v);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        job.setJarByClass(CommonFriends1MR.class);

        job.setMapperClass(CFMapper.class);
        job.setReducerClass(CFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path("conFriInput"));
        FileOutputFormat.setOutputPath(job,new Path("conFriOutput1"));


        boolean result = job.waitForCompletion(true);


        System.exit(result?0:1);


    }
}
