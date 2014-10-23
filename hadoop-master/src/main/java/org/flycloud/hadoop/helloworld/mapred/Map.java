package org.flycloud.hadoop.helloworld.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, DoubleWritable> output,
			Reporter reporter) throws IOException {
		
		String line = value.toString();
		String[] str = line.split(" ");
		int id = Integer.parseInt(str[0]);
		double temp = Double.parseDouble(str[1]);
		if (id <= 1000 && id >= 0 && temp < 100 && temp >= 0) {
			output.collect(new IntWritable(id), new DoubleWritable(temp));
		}
	}
}