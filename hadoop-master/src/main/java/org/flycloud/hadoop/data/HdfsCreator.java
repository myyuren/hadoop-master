package org.flycloud.hadoop.data;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.flycloud.hadoop.data.fields.DoubleField;
import org.flycloud.hadoop.data.fields.EnumRandomField;
import org.flycloud.hadoop.data.fields.IntField;
import org.flycloud.hadoop.data.fields.StringField;

public class HdfsCreator {
	private static byte[] rowSplitter = new byte[] { '\n' };
	private static byte[] colSplitter = new byte[] { '\t' };

	public static void create(FileSystem fs, String dbpath, Database db) throws IOException {
		Path path = new Path(dbpath);
		fs.mkdirs(path);
		for(String t : db.getTablesName()) {
			create(fs, dbpath, db.getTable(t), t);
		}
	}

	private static void create(FileSystem fs, String dbpath,
			Table table, String name) throws IOException {
		Path path = new Path(dbpath + "/" + name);
		fs.mkdirs(path);
		try {
			Path file = new Path(path + "/" + UUID.randomUUID().toString());
			FSDataOutputStream out = fs.create(file);
			for (int rows = 0; rows < table.getCount(); rows++) {
				int cols = 0;
				if (rows > 0) {
					out.write(rowSplitter);
				}
				for (String f : table.getFieldsName()) {
					if (cols++ != 0) {
						out.write(colSplitter);
					}
					out.write(table.getField(f).random());
				}
			}
			out.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

	public static void main(String[] args) throws IOException {
		EnumRandomField f1 = new EnumRandomField("男", "女");
		DoubleField f2 = new DoubleField();
		StringField f3 = new StringField();
		IntField f4 = new IntField(0, 100);
		
		int size = 100;
		Database db = new Database("mydb");
		Table user = db.createTable("user", size);
		user.createField("id", f1);
		user.createField("sex", f2);
		user.createField("x1", f3);
		user.createField("x2", f4);

		Table dept = db.createTable("dept", size);
		dept.createField("id", f4);
		dept.createField("sex", f3);
		dept.createField("x1", f2);
		dept.createField("x2", f1);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop1:8020");
		FileSystem fs = FileSystem.get(conf);
		HdfsCreator.create(fs, "/tmp/db5/", db);
	}
}
