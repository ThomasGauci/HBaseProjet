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
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * Classe exemple pour utiliser HBase
 * 
 * @author Pierre SAUNDERS
 * @version 1.0
 */
// @SuppressWarnings("deprecation")
public class HBaseClient {

	public static final void main(final String[] args) throws IOException {
		printSeperator("Hello World !");
		final HBaseClient hbc = new HBaseClient();
		hbc.listTables();
		hbc.removeTable("employe");
		hbc.createTable("employe", new String[] { "personal", "professional" });
		hbc.end();
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

	public final void removeTable(final String name) {
		// TODO implements me !
	}

	public final void createTable(final String name, final String... colNames) throws IOException {
		printSeperator("creating table " + name);
		final TableDescriptorBuilder tableDescriptor = new TableDescriptorBuilder(TableName.valueOf(name));

		for (final String colName : colNames)
			tableDescriptor.addFamily(new HColumnDescriptor(colName));

		admin.createTable(tableDescriptor);
		print("Table " + name + " created");
	}

	public final void end() {
		try {
			admin.close();
			conn.close();
		} catch (final Exception e) {
			// Nan
		}
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
