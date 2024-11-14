package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CompareDriver {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        initializeJob(conf, args);
        System.exit(0);
    }

    private static void initializeJob(Configuration conf, String[] args) throws Exception {
        conf.set("stopwords.path", args[1]);
        runJob(conf, args, "Word count", CompareMapper.class, CompareReducer.class, Text.class, IntWritable.class, args[0], args[1]);
    }

    private static void runJob(Configuration conf, String[] args, String jobName, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, Class<?> mapOutputKeyClass, Class<?> mapOutputValueClass, String inputPath, String outputPath) throws Exception {
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(CompareDriver.class);
        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        if (!job.waitForCompletion(true)) {
            System.exit(-1);
        }
    }
}
