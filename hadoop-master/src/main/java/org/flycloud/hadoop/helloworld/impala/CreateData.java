package org.flycloud.hadoop.helloworld.impala;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateData {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.0.199:8020");
		FileSystem hdfs = FileSystem.get(conf);
		Path hdfsFile = new Path("tmp/hivedata1");
		Random ran = new Random();
		int id = 0;
		double temp1 = 0;
		double temp2 = 0;
		long count = 100;
		try {
			FSDataOutputStream out = hdfs.create(hdfsFile);

			for (long i = 0; i < count; i++) {
				id = ran.nextInt(1000);
				temp1 = ran.nextDouble() * 100;
				temp2 = ran.nextDouble() * 100;
				out.write((id + "," + temp1 + ",x" + temp2 + "\n").getBytes());
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}