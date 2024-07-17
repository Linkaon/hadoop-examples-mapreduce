package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Show_All_Existing_Species_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] columns = value.toString().split("\t");
        if (columns.length > 3) {
            
            species.set(columns[3]);
            context.write(species, NullWritable.get());
        }
    }
}
