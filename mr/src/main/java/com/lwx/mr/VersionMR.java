package com.lwx.mr;

import com.lwx.domain.Version;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
20170308,黄渤,光环斗地主,8,360手机助手,0.1版本,北京
20170308,黄渤,光环斗地主,5,360手机助手,0.1版本,北京
20170308,黄渤,光环斗地主,7,360手机助手,0.1版本,北京
20170308,黄渤,光环斗地主,10,360手机助手,0.2版本,北京
20170308,黄渤,光环斗地主,9,360手机助手,0.2版本,北京
20170308,黄渤,光环斗地主,23,360手机助手,0.2版本,北京
20170308,黄渤,光环斗地主,22,360手机助手,0.2版本,北京
20170308,黄渤,光环斗地主,14,360手机助手,0.3版本,北京
20170308,黄渤,光环斗地主,13,360手机助手,0.3版本,北京
20170308,黄渤,光环斗地主,16,360手机助手,0.4版本,北京
20170308,黄渤,光环斗地主,18,360手机助手,0.4版本,北京
20170308,黄渤,光环斗地主,19,360手机助手,0.4版本,北京
20170308,黄渤,光环斗地主,15,360手机助手,0.4版本,北京
20170309,徐峥,光环斗地主,8,360手机助手,0.1版本,北京
20170309,徐峥,光环斗地主,5,360手机助手,0.1版本,北京
20170309,徐峥,光环斗地主,6,360手机助手,0.1版本,北京
20170309,徐峥,光环斗地主,10,360手机助手,0.2版本,北京
20170309,徐峥,光环斗地主,12,360手机助手,0.2版本,北京
20170309,徐峥,光环斗地主,11,360手机助手,0.3版本,北京
20170309,徐峥,光环斗地主,9,360手机助手,0.2版本,北京
20170309,徐峥,光环斗地主,23,360手机助手,0.2版本,北京
20170309,徐峥,光环斗地主,22,360手机助手,0.2版本,北京
20170309,徐峥,光环斗地主,14,360手机助手,0.3版本,北京
20170309,徐峥,光环斗地主,13,360手机助手,0.3版本,北京
20170309,徐峥,光环斗地主,16,360手机助手,0.4版本,北京
20170309,徐峥,光环斗地主,18,360手机助手,0.4版本,北京
20170309,徐峥,光环斗地主,19,360手机助手,0.5版本,北京
20170309,徐峥,光环斗地主,15,360手机助手,0.4版本,北京

*/
public class VersionMR extends Configured implements Tool {

    public static class VersionMapper extends Mapper<LongWritable, Text, Version, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            Version version=new Version(data[0],data[1],data[2],Integer.parseInt(data[3]),data[4],data[5],data[6]);
            context.write(version,NullWritable.get());
        }
    }

    public static class VersionReducer extends Reducer<Version,NullWritable,Text,NullWritable>{
        private String lastId;
        private String lastName;
        private String lastVersion;
        @Override
        protected void reduce(Version key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            for(NullWritable nul:values){
                if(lastVersion==null){
                    context.write(new Text(key.toString()), NullWritable.get());
                }
                else{
                    if(lastId.equals(key.getId()) && lastName.equals(key.getName())){
                        if(!lastVersion.equals(key.getVersion())){
                            context.write(new Text(key.toString()+","+lastVersion), NullWritable.get());

                        }
                    }

                        context.write(new Text(key.toString()), NullWritable.get());


                }
                lastId=key.getId();
                lastName=key.getName();
                lastVersion=key.getVersion();

            }



        }
    }

    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new VersionMR(), args);
        System.exit(run);
    }

    public int run(String[] args) throws Exception {

        // 指定hdfs相关的参数
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(VersionMR.class);

        // 指定mapper类和reducer类
        job.setMapperClass(VersionMapper.class);
        job.setReducerClass(VersionReducer.class);

        // 指定maptask的输出类型
        job.setMapOutputKeyClass(Version.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 指定reducetask的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 本地路径
        Path inputPath = new Path("versionInput");
        Path outputPath = new Path("versionOutput");

        // 指定该mapreduce程序数据的输入和输出路径
        // Path inputPath = new Path("/version/input");
        // Path outputPath = new Path("/version/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 最后提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        return waitForCompletion ? 0 : 1;
    }

}
