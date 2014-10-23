package org.flycloud.hadoop.data;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.flycloud.hadoop.data.fields.DoubleField;
import org.flycloud.hadoop.data.fields.EnumRandomField;
import org.flycloud.hadoop.data.fields.IntField;
import org.flycloud.hadoop.data.fields.StringField;

/**
 * 向hbase 插入数据
 * @author chenbaoyu
 *
 */
public class HbaseCreator {
	static Configuration config = new Configuration();
	static{
		config.set("fs.defaultFS", "hdfs://hadoop1:8020");
		config.set("hbase.rootdir", "hdfs://hadoop1:9000/hbase");
		config.set("hbase.cluster.distributed", "true");
		config.set("hbase.zookeeper.quorum", "hadoop2,hadoop3");
		config.set("dfs.permissions.superusergroup", "hadoop");
	}
	Configuration conf = HBaseConfiguration.create(config);

	public void createTable(HBaseAdmin admin, String tablename, Table table)
			throws IOException {
		if (admin.tableExists(tablename)) {
			System.out.println("表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (String c : table.getFieldsName()) {
				tableDesc.addFamily(new HColumnDescriptor(c));
			}
			admin.createTable(tableDesc);
			System.out.println("表创建成功！");
		}
	}

	public void createTables(Database db) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		for (String t : db.getTablesName()) {
			createTable(admin, t, db.getTable(t));
		}
		admin.close();
	}
	
	public void createData(HBaseAdmin admin, String tablename, Table tb) {
		try {
			HTable table = new HTable(conf, tablename);
			
			for (int i=0; i<tb.getCount(); i++) {
				System.out.println("---"+i);
				Put put = new Put(UUID.randomUUID().toString().getBytes());
				for (String fn : tb.getFieldsName()) {
					put.add(fn.getBytes(), null, tb.getField(fn).random());	
				}
				table.put(put);
			}
			
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createData(Database db) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		for (String t : db.getTablesName()) {
			createData(admin, t, db.getTable(t));
		}
		admin.close();
	}

	public static void main(String[] args) throws IOException {
		EnumRandomField f1 = new EnumRandomField("男", "女");
		DoubleField f2 = new DoubleField();
		StringField f3 = new StringField();
		IntField f4 = new IntField(0, 100);
		long start = System.currentTimeMillis();
		
		int size = 10000;
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
		
		HbaseCreator hc =new HbaseCreator();
		hc.createTables(db);
		hc.createData(db);
		long end = System.currentTimeMillis();
		long interval = end - start;
		System.out.println(interval+"------------------------");
	}

}
