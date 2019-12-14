package com.lwx.mr;

import com.lwx.domain.Score;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CourseScoreMR3 {

    public static class CsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Text k = new Text();
        DoubleWritable v = new DoubleWritable();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");
            double sum = 0;
            double avg = 0;

            k.set(data[0]);
            for (int i = 2; i < data.length; i++) {
                sum += Double.parseDouble(data[i]);
            }
            avg = new Double(sum / (data.length - 2));
            v.set(avg);

            

            context.write(k, v);


        }


    }


    public static class CsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        Text k = new Text();
        Text v = new Text();


        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            Double sum = new Double(0);


            for (DoubleWritable s : values) {
                sum += s.get();
                count++;
            }

            Double avg = (double)Math.round((sum / count)*10)/10;



            k.set(key);
            v.set( avg+"\t"+count);

            context.write(k,v);


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CourseScoreMR3.class);

        job.setMapperClass(CsMapper.class);
        job.setReducerClass(CsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path("courseScoreInput2"));
        FileOutputFormat.setOutputPath(job, new Path("courseScoreOutput2"));


        boolean result = job.waitForCompletion(true);


        System.exit(result ? 0 : 1);


    }
}
