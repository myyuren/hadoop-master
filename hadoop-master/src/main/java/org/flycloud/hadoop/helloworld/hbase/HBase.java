package org.flycloud.hadoop.helloworld.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {
	private static Configuration conf = null;
	private static Configuration config = new Configuration();
	static {
		
		config.set("fs.defaultFS", "hdfs://hadoop1:8020");
		config.set("hbase.rootdir", "hdfs://hadoop1:9000/hbase");
		config.set("hbase.cluster.distributed", "true");
		config.set("hbase.zookeeper.quorum", "hadoop2,hadoop3");
		config.set("dfs.permissions.superusergroup", "hadoop");
		conf = HBaseConfiguration.create(config);
	}

	public static void createTable(String tablename, String[] cfs)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("表创建成功！");
		}
		admin.close();
	}

	public void deleteTable(String tablename) throws IOException {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println("表删除成功！");
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	public void deleteRow(String tablename, String rowkey) throws IOException {
		HTable table = new HTable(conf, tablename);
		List<Delete> list = new ArrayList<Delete>();
		Delete d1 = new Delete(rowkey.getBytes());
		list.add(d1);
		table.delete(list);
		table.close();
		System.out.println("删除行成功！");
	}

	public static void selectRow(String tablename, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(rowKey.getBytes());
		Result rs = table.get(g);
		Cell[] cs = rs.rawCells();
		for (Cell c : cs) {
			System.out.print(new String(c.getRowArray()) + "");
			System.out.print(new String(c.getFamilyArray()) + ":");
			System.out.print(new String(c.getQualifierArray()) + "");
			System.out.print(c.getTimestamp() + "");
			System.out.print(new String(c.getValueArray()));
		}
		table.close();
	}

	public static void insert(String tablename) {
		try {
			HTable table = new HTable(conf, tablename);
			Put put = new Put("1234567892".getBytes());
			put.add("fml".getBytes(), "c1".getBytes(), "val921".getBytes());
			put.add("fml".getBytes(), "c2".getBytes(), "val922".getBytes());
			put.add("fml".getBytes(), "c3".getBytes(), "val923".getBytes());
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void scaner(String tablename) {
		long start = System.currentTimeMillis();
		try {
			conf = HBaseConfiguration.create(config);
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan();
//			FilterList filter = new FilterList(
//					FilterList.Operator.MUST_PASS_ALL);
//			filter.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
//					new BinaryComparator("1234567892".getBytes())));
//			FilterList filter1 = new FilterList(
//					FilterList.Operator.MUST_PASS_ONE);
//			filter1.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
//					new BinaryComparator("val921".getBytes())));
//			filter1.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
//					new BinaryComparator("val922".getBytes())));
//			filter.addFilter(filter1);
//			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Cell[] cs = r.rawCells();
				for (Cell c : cs) {
					String rowKey = new String(c.getRowArray(), c.getRowOffset(), c
							.getRowLength());
					if(rowKey.equals("707fe781-2878-434b-a2bb-25571da8e9c0"))
					{
						System.out.println("RowKey:"
								+ new String(c.getRowArray(), c.getRowOffset(), c
										.getRowLength()));
						System.out.println("Family:"
								+ new String(c.getFamilyArray(), c
										.getFamilyOffset(), c.getFamilyLength()));
						System.out.println("Qualifier:"
								+ new String(c.getQualifierArray(), c
										.getQualifierOffset(), c
										.getQualifierLength()));
						System.out.println("Timestamp:" + c.getTimestamp() + "");
						System.out.println("ValueArray:"
								+ new String(c.getValueArray(), c.getValueOffset(),
										c.getValueLength()));
					}
					break;
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		long interval = end-start;
		System.out.println("interval :" + interval);
	}

	public static void query() throws IOException {
		long start = System.currentTimeMillis();
		Configuration config = HBaseConfiguration.create();
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface hTable = connection.getTable("user");
		Scan scan = new Scan();
//		Filter pageFilter = getFilter();
//		scan.setFilter(pageFilter);
		ResultScanner rs = null;
		rs = hTable.getScanner(scan);
		if (rs != null) {
			for (Result result : rs) {
				for(KeyValue keyVal:result.list())
				{
					System.out.println(keyVal.getKeyString()+"-------------------");
					if(keyVal.getKeyString().contains("707fe781-2878-434b-a2bb-25571da8e9c0"))
					{
						long end = System.currentTimeMillis();
						long interval = end - start;
						System.out.println(interval+"**************************************");
						break;
					}
				}
			}
		}
	}

	private static Filter getFilter() {
		FilterList fl = new FilterList();
		Filter pf = new PageFilter(10);
		fl.addFilter(pf);
		return fl;
	}

	// 网点 贷款账号
	private static Filter getFilter1() {
		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		Filter pf = new PageFilter(1000);
		fl.addFilter(pf);
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("ISOUTTABLELOAN"),CompareOp.EQUAL, Bytes.toBytes("F")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("813")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("816")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("deptcode"),CompareOp.EQUAL, Bytes.toBytes("020740")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("citycode"),CompareOp.EQUAL, Bytes.toBytes("907")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("busimanager"),CompareOp.EQUAL, Bytes.toBytes("02074000003")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("LOANACCOUNT"),CompareOp.EQUAL, Bytes.toBytes("9070107104348080085277")));
		return fl;
	}

	// 网点 贷款形态
	private static Filter getFilter2() {
		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		Filter pf = new PageFilter(1000);
		fl.addFilter(pf);
		FilterList f5 = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for (String string : new String[] { "1", "2", "3", "4", "5" }) {
			Filter filter = new SingleColumnValueFilter("d".getBytes(),
					Bytes.toBytes("fiveclass"), CompareOp.EQUAL,
					Bytes.toBytes(string));
			f5.addFilter(filter);
		}
		fl.addFilter(f5);

		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("ISOUTTABLELOAN"),CompareOp.EQUAL, Bytes.toBytes("F")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("813")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("816")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("deptcode"),CompareOp.EQUAL, Bytes.toBytes("020740")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("citycode"),CompareOp.EQUAL, Bytes.toBytes("907")));
		
		return fl;
	}

	// 县级 贷款形态
	private static Filter getFilter3() {
		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		Filter pf = new PageFilter(1000);
		fl.addFilter(pf);
		FilterList f5 = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for (String string : new String[] { "1", "2", "3", "4", "5" }) {
			Filter filter = new SingleColumnValueFilter("b".getBytes(),
					Bytes.toBytes("fiveclass"), CompareOp.EQUAL,
					Bytes.toBytes(string));
			f5.addFilter(filter);
		}
		fl.addFilter(f5);

		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("ISOUTTABLELOAN"),CompareOp.EQUAL, Bytes.toBytes("F")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("813")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("BMCFG"),CompareOp.NOT_EQUAL, Bytes.toBytes("816")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("upgroupid"),CompareOp.EQUAL, Bytes.toBytes("020740")));
		fl.addFilter(new SingleColumnValueFilter("d".getBytes(),Bytes.toBytes("citycode"),CompareOp.EQUAL, Bytes.toBytes("907")));
		
		return fl;
	}

	public static void main(String[] args) throws IOException {
		// createTable("table2", new String[] { "fml" });
		// insert("table2");
		 scaner("user");
		// selectRow("datatsv", "0200A00000001");
		//query();
	}

}
