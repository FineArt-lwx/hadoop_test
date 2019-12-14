package com.lwx.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class CommonFriends2MR {

    public static class CFMapper extends Mapper<LongWritable, Text, Text,Text> {
        Text k=new Text();
        Text v=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] data=line.split("\t");

            String friend=data[0];
            String[] persons=data[1].split("-");


            Arrays.sort(persons);
            for(int i=0;i<persons.length-1;i++){
                for(int j=i+1;j<persons.length;j++){

                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));

                }
            }


        }
    }


    public static class CFReducer extends Reducer<Text,Text,Text,Text> {
        Text v=new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer("");
            for(Text p:values){
                sb.append(p).append(" ");
            }
            v.set(sb.toString());
            context.write(key,v);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        job.setJarByClass(CommonFriends2MR.class);

        job.setMapperClass(CFMapper.class);
        job.setReducerClass(CFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path("conFriOutput1"));
        FileOutputFormat.setOutputPath(job,new Path("conFriOutputResult"));


        boolean result = job.waitForCompletion(true);


        System.exit(result?0:1);


    }
}
