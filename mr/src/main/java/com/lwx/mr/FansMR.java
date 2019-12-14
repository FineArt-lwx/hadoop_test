package com.lwx.mr;


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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FansMR {
    public static class FansMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable v=new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] data=line.split(":");

            String person=data[0];
            String[] friends=data[1].split(",");

            for(String friend:friends){


                String k = combineStr(person, friend);
                context.write(new Text(k),v);
            }


        }

        private String combineStr(String person, String friend) {
            if(person.compareTo(friend)>0){
                return friend+"-"+person;
            }else {
                return person+"-"+friend;
            }
        }


    }


    public static class FansReducer extends Reducer<Text,IntWritable,Text, NullWritable> {
        Text v=new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count=0;
            for(IntWritable p:values){
                count++;
            }
            if(count==2){
                context.write(key,NullWritable.get());
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        job.setJarByClass(FansMR.class);

        job.setMapperClass(FansMapper.class);
        job.setReducerClass(FansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path("fansInput"));
        FileOutputFormat.setOutputPath(job,new Path("fansOutput"));


        boolean result = job.waitForCompletion(true);


        System.exit(result?0:1);


    }
}
