package miage.unice.fr;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
		hbc.listColumn("employe");
		hbc.deleteRow("employe");
		hbc.listColumn("employe");

		// TODO à extraire du dtd !
		hbc.deleteTable("Invoice");
		final String[] xmlColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		final String[] xmlsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		final String filepath = hbc.getClass().getClassLoader().getResource("Invoice.xml").getFile();
		hbc.insertXML("Invoice", filepath, xmlColnames, xmlsub_orderLineColnames);

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

	public final void deleteRow(final String name) throws IOException {
		printSeperator("DeleteRow " + name);
		final Table table = conn.getTable(TableName.valueOf(name));
		final Delete delete = new Delete("row1".getBytes());

		table.delete(delete);
		print("row deleted");
		table.close();
	}

	public final void listColumn(final String name) throws IOException {
		printSeperator("Select " + name);
		final Table table = conn.getTable(TableName.valueOf(name));
		final Scan scan = new Scan();
		scan.addColumn("personal".getBytes(), "name".getBytes());
		scan.addColumn("personal".getBytes(), "city".getBytes());
		scan.addColumn("professional".getBytes(), "designation".getBytes());
		scan.addColumn("professional".getBytes(), "salary".getBytes());
		final ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next())
			print("Found row :", result);
		scanner.close();
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

	public final void insertXML(final String table_name, final String filename, final String[] xmlColnames,
			final String[] xmlsub_orderLineColnames) throws IOException {

		printSeperator("inserting xml", filename, "into", table_name);
		final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		Document document;
		try {
			builder = factory.newDocumentBuilder();
			document = builder.parse(new File(filename));
		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
			return;
		}

		final TableName tableName = TableName.valueOf(table_name);
		if (!admin.tableExists(tableName)) {
			createTable(table_name, xmlColnames);
		}

		final Table table = conn.getTable(tableName);

		final Element root = document.getDocumentElement();
		final NodeList rootNodes = root.getChildNodes();
		final int size = rootNodes.getLength();
		// entre chaque noeud il y un noeud #text = ""
		for (int i = 1; i < size; i += 2) {
			print("Node", i);
			final Node node = rootNodes.item(i);
			final NodeList columns = node.getChildNodes();
			final int nodeSize = columns.getLength();

			Put p = null;
			int j = 1;
			for (; j < xmlColnames.length * 2 - 2; j += 2) {
				final Node column = columns.item(j);
				if (j == 1) {
					p = new Put(Bytes.toBytes(node.getTextContent()));
				} else
					p.addColumn(Bytes.toBytes(column.getNodeName()), Bytes.toBytes(column.getNodeName()),
							Bytes.toBytes(column.getTextContent()));
			}

			for (; j < nodeSize; j += 2) {
				final Node column = columns.item(j);
				p.addColumn(Bytes.toBytes(xmlColnames[xmlColnames.length - 1]), Bytes.toBytes(column.getNodeName()),
						Bytes.toBytes(column.getTextContent()));
			}
			table.put(p);
		}

		table.close();
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
