package miage.unice.fr;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Classe exemple pour utiliser HBase
 * 
 * @author Pierre SAUNDERS
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class HBaseClient {

	public static final void main(final String[] args) throws IOException {
		printSeperator("Starting");
		final HBaseClient hbc = new HBaseClient();
		hbc.listTables();
		hbc.deleteTable("employe");
		hbc.createTable("employe", new String[] { "personal", "professional" });
		hbc.getData("employe");
		hbc.insertData("employe");
		hbc.getData("employe");
		hbc.updateData("employe");
		hbc.getData("employe");
		printSeperator("Exiting");
	}

	private final Configuration cfg;
	private final Connection conn;
	private final Admin admin;

	public HBaseClient() throws IOException {
		printSeperator("Creating configuration");
		cfg = HBaseConfiguration.create();
		cfg.clear();
		final String path = getClass().getClassLoader().getResource("hbase-site.xml").getPath();

		cfg.addResource(new Path(path));

		Arrays.asList(
				new String[] { "hbase.zookeeper.quorum", "hbase.client.retries.number", "zookeeper.session.timeout" })
				.forEach(name -> print(name, cfg.get(name)));

		conn = ConnectionFactory.createConnection(cfg);
		admin = conn.getAdmin();
		printSeperator("Connected !");
	}

	public final void listTables() throws IOException {
		printSeperator("List tables");
		for (final HTableDescriptor td : admin.listTables())
			print("TableName", td.getNameAsString());
	}

	public final void getData(final String tName) throws IOException {
		final Table table = conn.getTable(TableName.valueOf(tName));
		final Get g = new Get(Bytes.toBytes("row1"));
		final Result result = table.get(g);
		final byte[] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		final byte[] value1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		final String name = Bytes.toString(value);
		final String city = Bytes.toString(value1);
		print("name:", name, "city:", city);
	}

	public final void updateData(final String name) throws IOException {
		final Table table = conn.getTable(TableName.valueOf(name));

		final Put p = new Put(Bytes.toBytes("row1"));

		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("helloya"));
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"));
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("manager"));
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("50000"));

		table.put(p);
		print("data updated");

		table.close();
	}

	public final void insertData(final String name) throws IOException {
		final Table table = conn.getTable(TableName.valueOf(name));

		final Put p = new Put(Bytes.toBytes("row1"));

		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("raju"));
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"));
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("manager"));
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("50000"));

		table.put(p);
		print("data inserted");
		table.close();
	}

	public final void deleteTables(final String... names) throws IOException {
		for (final String name : names)
			deleteTable(name);
	}

	public final void deleteTable(final String name) throws IOException {
		printSeperator("Deleting table", name);
		final TableName tName = TableName.valueOf(name);
		if (admin.tableExists(tName)) {
			admin.disableTable(tName);
			admin.deleteTable(tName);
			print("Table", name, "deleted");
		} else {
			print("Table", name, "not in database");
		}
	}

	public final void createTable(final String name, final String... colNames) throws IOException {
		printSeperator("Creating table", name);
		final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));

		for (final String colName : colNames)
			tableDescriptor.addFamily(new HColumnDescriptor(colName));

		admin.createTable(tableDescriptor);
		print("Table", name, "created");
	}

	public static final void printSeperator(final String... s) {
		final List<String> list = new LinkedList<String>(Arrays.asList(s));
		list.add(0, "\n----------------");
		list.add("----------------\n");
		print(list.toArray(new Object[list.size()]));
	}

	public static final void print(final Object... o) {
		int i = 0;
		for (; i < o.length - 1; i++)
			System.out.print(o[i] + " ");
		System.out.println(o[i]);
	}
}
