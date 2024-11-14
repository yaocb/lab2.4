package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int result = 0;
    private static final Logger logger = LoggerFactory.getLogger(CompareReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : values) {
            context.write(key, val);
            result += val.get();
        }
    }

    @Override
    protected void cleanup(Context context) 
        throws IOException, InterruptedException {
        String FinalResult = "There are " + result + " times that weekly-yield interest is larger than daily-yield interest";
        context.write(new Text(FinalResult), null);
    }
}
