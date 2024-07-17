package com.opstty.mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Maximum_Height_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text treeKind = new Text();
    private DoubleWritable height = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into columns
        String[] columns = value.toString().split("\t");
        if (columns.length > 6) {
            try {
                // Extract the "GENRE" value (third column) and "HAUTEUR" value (seventh column)
                treeKind.set(columns[2]);
                height.set(Double.parseDouble(columns[6]));
                context.write(treeKind, height);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid height values
            }
        }
    }
}
