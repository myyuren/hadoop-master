package org.flycloud.hadoop.helloworld.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements
		Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
	public void reduce(IntWritable key, Iterator<DoubleWritable> values,
			OutputCollector<IntWritable, DoubleWritable> output,
			Reporter reporter) throws IOException {
		double maxTemp = 0;
		while (values.hasNext()) {
			maxTemp = Math.max(maxTemp, values.next().get());
		}
		output.collect(key, new DoubleWritable(maxTemp));
	}
	
}