package miage.unice.fr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import org.apache.hadoop.hbase.client.TableDescriptor;
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
@SuppressWarnings({ "deprecation", "unchecked" })
public class HBaseClient {

	public static final void main(final String[] args) throws IOException {
		printSeparator("Starting");
		final HBaseClient hbc = new HBaseClient();
		hbc.listTables();
		hbc.deleteTable("employe");
		hbc.createTable("employe", new String[] { "personal", "professional" });
		hbc.listColumn("employe");

		hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));
		hbc.insertData("employe", "row1", new Triple<String>("personal", "name", "raju"),
				new Triple<String>("personal", "city", "hyderabad"),
				new Triple<String>("professional", "designation", "manager"),
				new Triple<String>("professional", "salary", "50000"));

		hbc.scanData("employe");
		hbc.scanData("employe", new Tuple<String>("personal", "name"), new Tuple<String>("professional", "salary"));

		hbc.updateData("employe", "row1", new Triple<String>("personal", "city", "helloya"));
		hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));

		hbc.deleteRow("employe", "row1");
		hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));

		// TODO à extraire du dtd !
		hbc.deleteTable("Invoice");
		final String[] xmlColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		final String[] xmlsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		final String xmlfilepath = hbc.getClass().getClassLoader().getResource("Invoice.xml").getFile();
		hbc.insertXML("Invoice", xmlfilepath, xmlColnames, xmlsub_orderLineColnames);

		String csvfilepath = hbc.getClass().getClassLoader().getResource("person_0_0.csv").getFile();
		hbc.deleteTable("Person");
		hbc.insertCSV("Person", csvfilepath, "\\|");
		hbc.scanData("Person");

		csvfilepath = hbc.getClass().getClassLoader().getResource("Product.csv").getFile();
		hbc.deleteTable("Product");
		hbc.insertCSV("Product", csvfilepath);
		hbc.scanData("Product");

		printSeparator("Exiting");
	}

	private final Configuration cfg;
	private final Connection conn;
	private final Admin admin;

	public HBaseClient() throws IOException {
		printSeparator("Creating configuration");
		cfg = HBaseConfiguration.create();
		cfg.clear();
		final String path = getClass().getClassLoader().getResource("hbase-site.xml").getPath();

		cfg.addResource(new Path(path));

		Arrays.asList(
				new String[] { "hbase.zookeeper.quorum", "hbase.client.retries.number", "zookeeper.session.timeout" })
				.forEach(name -> print(name, cfg.get(name)));

		conn = ConnectionFactory.createConnection(cfg);
		admin = conn.getAdmin();
		printSeparator("Connected !");
	}

	public final void listTables() throws IOException {
		printSeparator("List tables");
		for (final HTableDescriptor td : admin.listTables())
			print("TableName", td.getNameAsString());
	}

	public final void getData(final String name, final String row, final Tuple<String>... sts) throws IOException {
		printSeparator("getData", name);
		final TableName tableName = TableName.valueOf(name);
		if (admin.tableExists(tableName)) {
			final Table table = conn.getTable(tableName);
			final Get g = new Get(row.getBytes());
			final Result result = table.get(g);
			for (final Tuple<String> st : sts)
				print(st.a, "-", st.b, ":", Bytes.toString(result.getValue(st.a.getBytes(), st.b.getBytes())));
		} else
			print("La table", name, "n'existe pas dans la base");
	}

	public final void updateData(final String name, final String row, final Triple<String>... sts) throws IOException {
		printSeparator("updateData", name);
		final Table table = conn.getTable(TableName.valueOf(name));

		final Put p = new Put(row.getBytes());

		for (final Triple<String> ts : sts)
			p.addColumn(ts.a.getBytes(), ts.b.getBytes(), ts.c.getBytes());

		table.put(p);
		print("data updated");
		table.close();
	}

	public final void insertData(final String name, final String row, final Triple<String>... sts) throws IOException {
		printSeparator("insertData", name);
		final Table table = conn.getTable(TableName.valueOf(name));

		final Put p = new Put(row.getBytes());

		for (final Triple<String> ts : sts)
			p.addColumn(ts.a.getBytes(), ts.b.getBytes(), ts.c.getBytes());

		table.put(p);
		print("data inserted");
		table.close();
	}

	public final void deleteRow(final String name, final String row) throws IOException {
		printSeparator("DeleteRow", name);
		final Table table = conn.getTable(TableName.valueOf(name));
		final Delete delete = new Delete(row.getBytes());

		table.delete(delete);
		print("row deleted");
		table.close();
	}

	public final void listColumn(final String name) throws IOException {
		printSeparator("listColumn", name);
		final TableName tableName = TableName.valueOf(name);
		if (admin.tableExists(tableName)) {
			final TableDescriptor td = admin.getDescriptor(tableName);
			for (final var cf : td.getColumnFamilies())
				print("Column Family :", cf.getNameAsString());
		} else
			print("La table", name, "n'existe pas dans la base");
	}

	public final void scanData(final String name) throws IOException {
		scanData(name, new Tuple[] {});
	}

	public final void scanData(final String name, final Tuple<String>... sts) throws IOException {
		printSeparator("scanData", name);
		final TableName tableName = TableName.valueOf(name);
		if (admin.tableExists(tableName)) {
			final Table table = conn.getTable(tableName);
			final Scan scan = new Scan();
			for (final Tuple<String> st : sts)
				scan.addColumn(st.a.getBytes(), st.b.getBytes());
			final ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner.next())
				print("Found row :", result);
			scanner.close();
		} else
			print("La table", name, "n'existe pas dans la base");
	}

	public final void deleteTables(final String... names) throws IOException {
		for (final String name : names)
			deleteTable(name);
	}

	public final void deleteTable(final String name) throws IOException {
		printSeparator("Deleting table", name);
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
		printSeparator("Creating table", name);
		final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));

		for (final String colName : colNames)
			tableDescriptor.addFamily(new HColumnDescriptor(colName));

		admin.createTable(tableDescriptor);
		print("Table", name, "created");
	}

	public final void insertXML(final String table_name, final String filename, final String[] xmlColnames,
			final String[] xmlsub_orderLineColnames) throws IOException {

		printSeparator("inserting xml", filename, "into", table_name);
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
			final Node node = rootNodes.item(i);
			final NodeList columns = node.getChildNodes();
			final int nodeSize = columns.getLength();

			Put p = null;
			int j = 1;
			for (; j < xmlColnames.length * 2 - 2; j += 2) {
				final Node column = columns.item(j);
				if (j == 1) {
					p = new Put(node.getTextContent().getBytes());
				} else
					p.addColumn(column.getNodeName().getBytes(), column.getNodeName().getBytes(),
							column.getTextContent().getBytes());
			}

			for (; j < nodeSize; j += 2) {
				final Node column = columns.item(j);
				p.addColumn(xmlColnames[xmlColnames.length - 1].getBytes(), column.getNodeName().getBytes(),
						column.getTextContent().getBytes());
			}
			table.put(p);
		}

		table.close();
	}

	public final void insertCSV(final String table_name, final String filepath) throws IOException {
		insertCSV(table_name, filepath, ",");
	}

	public final void insertCSV(final String table_name, final String filepath, final String separator)
			throws IOException {

		printSeparator("inserting csv", filepath, "into", table_name);
		final TableName tableName = TableName.valueOf(table_name);

		final Table table = conn.getTable(tableName);

		try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			String line = "";
			String headers[] = null;
			for (int index = 0; (line = br.readLine()) != null; index++) {
				final String[] data = line.split(separator);
				if (index == 0) {
					headers = new String[data.length];
					for (byte i = 0; i < data.length; i++)
						headers[i] = data[i];
					if (!admin.tableExists(tableName)) {
						createTable(table_name, headers);
					}
				} else {
					final Put p = new Put(data[0].getBytes());
					for (byte j = 1; j < headers.length; j++)
						p.addColumn(headers[j].getBytes(), headers[j].getBytes(), data[j].getBytes());
					table.put(p);
				}
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
		table.close();
	}

	public static final void printSeparator(final String... s) {
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
