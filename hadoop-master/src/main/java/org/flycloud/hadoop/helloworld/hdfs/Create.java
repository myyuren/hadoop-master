package org.flycloud.hadoop.helloworld.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Create {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		FileSystem hdfs = FileSystem.get(conf);
		Path hdfsFile = new Path("file");
		Random ran = new Random();
		int id = 0;
		double temp = 0;
		long count = 10000;
		try {
			FSDataOutputStream out = hdfs.create(hdfsFile);

			for (long i = 0; i < count; i++) {
				id = ran.nextInt(1000);
				temp = ran.nextDouble() * 100;
				out.write((id + " " + temp + "\n").getBytes());
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}