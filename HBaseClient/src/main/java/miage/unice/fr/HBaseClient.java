package miage.unice.fr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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
				.forEach(name -> print(name, cfg.getStrings(name)[0]));

		conn = ConnectionFactory.createConnection(cfg);
		admin = conn.getAdmin();
		printSeperator("Connected !");
	}

	public final void listTables() throws IOException {
		printSeperator("List tables");
		for (final HTableDescriptor td : admin.listTables())
			print("TableName", td.getNameAsString());
	}

	public final void deleteTable(final String name) throws IOException {
		printSeperator("Deleting table " + name);
		final TableName tName = TableName.valueOf(name);
		admin.disableTable(tName);
		admin.deleteTable(tName);
		print("Table", name, "deleted");
	}

	public final void createTable(final String name, final String... colNames) throws IOException {
		printSeperator("Creating table " + name);
		final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));

		for (final String colName : colNames)
			tableDescriptor.addFamily(new HColumnDescriptor(colName));

		admin.createTable(tableDescriptor);
		print("Table", name, "created");
	}

	private static final void printSeperator(final String name) {
		print("\n---------------- " + name + " ----------------\n");
	}

	public static final void print(final Object... o) {
		int i = 0;
		for (; i < o.length - 1; i++)
			System.out.print(o[i] + " ");
		System.out.println(o[i] + " ");
	}
}
