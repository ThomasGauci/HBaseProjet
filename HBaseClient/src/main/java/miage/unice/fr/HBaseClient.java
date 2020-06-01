package miage.unice.fr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
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
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
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
		hbc.listColumn("Invoice");
		hbc.query7();
		hbc.listTables();
		// hbc.scanData("Order");
		// hbc.deleteTable("employe");
		// hbc.createTable("employe", new String[] { "personal", "professional" });
		// hbc.listColumn("employe");

		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));
		// hbc.insertData("employe", "row1", new Triple<String>("personal", "name", "raju"),
		// 		new Triple<String>("personal", "city", "hyderabad"),
		// 		new Triple<String>("professional", "designation", "manager"),
		// 		new Triple<String>("professional", "salary", "50000"));

		// hbc.scanData("employe");
		// hbc.scanData("employe", new Tuple<String>("personal", "name"), new Tuple<String>("professional", "salary"));

		// hbc.updateData("employe", "row1", new Triple<String>("personal", "city", "helloya"));
		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));

		// hbc.deleteRow("employe", "row1");
		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new Tuple<String>("personal", "city"));

		// final String[] xmlColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		// final String[] xmlsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		// final String xmlfilepath = hbc.getClass().getClassLoader().getResource("Invoice.xml").getFile();
		// hbc.insertXML("Invoice", xmlfilepath, xmlColnames, xmlsub_orderLineColnames);

		// String csvfilepath = hbc.getClass().getClassLoader().getResource("person_0_0.csv").getFile();
		// hbc.deleteTable("Person");
		// hbc.insertCSV("Person", csvfilepath, "\\|");
		// hbc.scanData("Person");

		// csvfilepath = hbc.getClass().getClassLoader().getResource("Product.csv").getFile();
		// hbc.deleteTable("Product");
		// hbc.insertCSV("Product", csvfilepath);

		// csvfilepath = hbc.getClass().getClassLoader().getResource("Feedback.csv").getFile();
		// hbc.deleteTable("Feedback");
		// hbc.insertCSV("Feedback", csvfilepath, "\\|", new String[] { "asin", "PersonId", "feedback" });

		// csvfilepath = hbc.getClass().getClassLoader().getResource("Vendor.csv").getFile();
		// hbc.deleteTable("Vendor");
		// hbc.insertCSV("Vendor", csvfilepath);

		// final String[] jsonColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		// final String[] jsonsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		// final String filepathjson = hbc.getClass().getClassLoader().getResource("Order.json").getFile();
		// hbc.insertJSON("Order", filepathjson, jsonColnames, jsonsub_orderLineColnames);
		
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
			for (final ColumnFamilyDescriptor cf : td.getColumnFamilies())
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

	public final void insertJSON(final String table_name, final String filename, final String[] jsonColnames,
			final String[] jsonsub_orderLineColnames) throws IOException {

		printSeparator("inserting JSON", filename, "into", table_name);
		final TableName tableName = TableName.valueOf(table_name);
		if (!admin.tableExists(tableName)) {
			createTable(table_name, jsonColnames);
		}

		final Table table = conn.getTable(tableName);

		Put p = null;

		// lecture du fichier texte
		try {
			final InputStream ips = new FileInputStream(filename);
			final InputStreamReader ipsr = new InputStreamReader(ips);
			final BufferedReader br = new BufferedReader(ipsr);
			String ligne;
			while ((ligne = br.readLine()) != null) {
				final Gson gson = new Gson();
				final JsonObject jsonObject = gson.fromJson(ligne, JsonObject.class);

				p = new Put((jsonObject.get("OrderId").getAsString()).getBytes());

				p.addColumn(jsonColnames[0].getBytes(), jsonColnames[0].getBytes(),
						(jsonObject.get("OrderId").getAsString()).getBytes());
				p.addColumn(jsonColnames[1].getBytes(), jsonColnames[1].getBytes(),
						(jsonObject.get("PersonId").getAsString()).getBytes());
				p.addColumn(jsonColnames[2].getBytes(), jsonColnames[2].getBytes(),
						(jsonObject.get("OrderDate").getAsString()).getBytes());
				p.addColumn(jsonColnames[3].getBytes(), jsonColnames[3].getBytes(),
						(jsonObject.get("TotalPrice").getAsString()).getBytes());

				final JsonArray items = jsonObject.get("Orderline").getAsJsonArray();

				for (int i = 0; i < items.size(); i++) {
					final JsonObject jobject = items.get(i).getAsJsonObject();
					p.addColumn(jsonColnames[4].getBytes(), jsonsub_orderLineColnames[0].getBytes(),
							(jobject.get("productId").getAsString()).getBytes());
					p.addColumn(jsonColnames[4].getBytes(), jsonsub_orderLineColnames[1].getBytes(),
							(jobject.get("asin").getAsString()).getBytes());
					p.addColumn(jsonColnames[4].getBytes(), jsonsub_orderLineColnames[2].getBytes(),
							(jobject.get("title").getAsString()).getBytes());
					p.addColumn(jsonColnames[4].getBytes(), jsonsub_orderLineColnames[3].getBytes(),
							(jobject.get("price").getAsString()).getBytes());
					p.addColumn(jsonColnames[4].getBytes(), jsonsub_orderLineColnames[4].getBytes(),
							(jobject.get("brand").getAsString()).getBytes());
				}
				table.put(p);
			}
			br.close();
		} catch (final Exception e) {
			System.out.println(e.toString());
		}
		table.close();
	}

	public final void insertCSV(final String table_name, final String filepath, final String[] headers)
			throws IOException {
		insertCSV(table_name, filepath, ",", headers);
	}

	public final void insertCSV(final String table_name, final String filepath, final String separator,
			final String[] headers) throws IOException {

		printSeparator("inserting csv", filepath, "into", table_name);
		final TableName tableName = TableName.valueOf(table_name);
		if (!admin.tableExists(tableName)) {
			createTable(table_name, headers);
		}

		final Table table = conn.getTable(tableName);

		try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			String line = "";
			while ((line = br.readLine()) != null) {
				final String[] data = line.split(separator);
				final Put p = new Put(data[0].getBytes());
				for (byte j = 1; j < headers.length; j++)
					p.addColumn(headers[j].getBytes(), headers[j].getBytes(), data[j].getBytes());
				table.put(p);
			}
		} catch (final IOException e) {
			e.printStackTrace();
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
	public final void query7(){
		printSeparator("QUERY 7");
		Table table = null;
		ResultScanner rScanner = null;
		try {
			// Pour les produits d'un vendeur donné (UK_Gear inutile dans l'exemple)

			// Invoice personId > Order personId  > Customer personId > feedback personId = feedback asin > product asin = product brand > vendor vendor
			TableName tableName = TableName.valueOf("Vendor");
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			table = conn.getTable(tableName);

			Scan scan = new Scan();
			RowFilter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("UK_Gear")));

			scan.setFilter(filter);
			rScanner = table.getScanner(scan);
			for (Result res : rScanner) {
				print("res");
				byte[] val = res.getValue(Bytes.toBytes("vendor"), Bytes.toBytes("vendor"));
				System.out.println("Row-value vendor: " + Bytes.toString(val));
				System.out.println(res);
			}
			// ---------------------------------------------------------------------------------------------------------------------
			// On récupère le nom du vendeur , mtn on cherche cette marque dans brand de la table product
			tableName = TableName.valueOf("Product");
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			table = conn.getTable(tableName);
			
			Scan scanProduct = new Scan();
			// Ici c'est normalement la marque
			RowFilter filterProduct = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("B005FUKW6M")));

			scanProduct.setFilter(filterProduct);
			// Submit a scan request.
			rScanner = table.getScanner(scanProduct);
			String asinProduct ="";
			for (Result res : rScanner) {
				print("res");
				byte[] val = res.getValue(Bytes.toBytes("asin"), Bytes.toBytes("asin"));
				System.out.println("Row-value  asin product : " + Bytes.toString(val));
				System.out.println(res);
				asinProduct = Bytes.toString(val);
			}
			asinProduct = "B005FUKW6M"; // car je ne recupère pas encore le asin car je n'ai pas la marque
			// ---------------------------------------------------------------------------------------------------------------------
			// On récupère le asin du produit , mtn on cherche le feedback qui contient ce asin afin d'obtenir les personId
			tableName = TableName.valueOf("Feedback");
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			table = conn.getTable(tableName);
			
			Scan scanFeedbackPersonId = new Scan();
			RowFilter filterFeedbackPersonId = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(asinProduct)));

			scanFeedbackPersonId.setFilter(filterFeedbackPersonId);

			rScanner = table.getScanner(scanFeedbackPersonId);
			String personId = "";
			for (Result res : rScanner) {
				print("res");
				byte[] val = res.getValue(Bytes.toBytes("PersonId"), Bytes.toBytes("PersonId"));
				System.out.println("Row-value person id feedback : " + Bytes.toString(val));
				System.out.println(res);
				personId =  Bytes.toString(val);
			}
			// ---------------------------------------------------------------------------------------------------------------------
			// On a le personId pour récupèrer la date dans la table invoice ( il faudrait aussi recuperer le price en fonction du asin afin d'avoir un orderDate lié a un prix afin de calculer si il y a eu une baisse)
			tableName = TableName.valueOf("Invoice");
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			table = conn.getTable(tableName);
			
			Scan scanInvoice = new Scan();
			RowFilter filterInvoice = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(personId)));

			scanInvoice.setFilter(filterInvoice);

			rScanner = table.getScanner(scanInvoice);
			for (Result res : rScanner) {
				print("res");
				byte[] val = res.getValue(Bytes.toBytes("OrderDate"), Bytes.toBytes("OrderDate"));
				System.out.println("Row-value invoice orderdate : " + Bytes.toString(val));
				System.out.println(res);
			}
			// ---------------------------------------------------------------------------------------------------------------------
			// analyse des avis sur ces articles pour voir s'il y a des sentiments négatifs en fonction du asin
			tableName = TableName.valueOf("Feedback");
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			table = conn.getTable(tableName);
			
			Scan scanFeedbackAsin = new Scan();
			RowFilter filterFeedbackAsin = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("B005FUKW6M")));

			scanFeedbackAsin.setFilter(filterFeedbackAsin);
			rScanner = table.getScanner(scanFeedbackAsin);
			for (Result res : rScanner) {

				byte[] PersonId = res.getValue(Bytes.toBytes("PersonId"), Bytes.toBytes("PersonId"));
				System.out.println("Row-value person id feedback: " + Bytes.toString(PersonId) + "\n");				
				System.out.println(res);
			}
			// ---------------------------------------------------------------------------------------------------------------------
			printSeparator("La table : " + tableName + " " + admin.tableExists(tableName));
			Scan scanFeedback = new Scan();
			RowFilter filterFeedback = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("B00005OU5T")));

			scanInvoice.setFilter(filterFeedback);
			rScanner = table.getScanner(scanFeedback);
			int compteurNoteNegatif = 0;
			int i =0;
			for (Result res : rScanner) {
				print("res");
				byte[] feedback = res.getValue(Bytes.toBytes("feedback"), Bytes.toBytes("feedback"));
				System.out.println("Row-value feedback notes : " + Bytes.toString(feedback) + "\n");				
				String note = Bytes.toString(feedback).substring(1,2);
				print(" Note feedback : "+ note + "\n");
				i++;
				if(  Integer.parseInt(note) < 3){
					compteurNoteNegatif++;
				}
				System.out.println(res);
			}
			print("Nombre d'avis negatif : " + compteurNoteNegatif + " ce qui donne un poucentage de " + (i/compteurNoteNegatif) + " % d'avis negatif");
			// jpp il est 3h40 les données des tables sont claqués, je vais dormir

			table.close();
		} catch (IOException  e) {
			print("Single column value filter failed " ,e);
		}
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
