package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class CompareMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(CompareMapper.class);
    private boolean firstLine = true;
    private Text date = new Text();
    private final static IntWritable Day = new IntWritable(0);
    private final static IntWritable Week = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }
        if (firstLine && line.contains("mfd_date")) {
            firstLine = false;
            return;
        }

        String[] columns = line.split(",");
        if (columns.length < 3) {
            return;
        }

        String Date = columns[0].trim();
        double dailyYield = Double.parseDouble(columns[1]);
        double weeklyYield = Double.parseDouble(columns[2]);
        double convert = 100 * (Math.pow((dailyYield / 10000 + 1), 365) - 1);

        date.set(Date);
        if (convert > weeklyYield) {
            context.write(date, Day);
        } else {
            context.write(date, Week);
        }
    }
}
