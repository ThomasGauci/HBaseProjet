package miage.unice.fr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
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

	private static final int insertedLineMax = 2000;
	private static final DecimalFormat df = new DecimalFormat("0.00");

	public static final void main(final String[] args) throws IOException {
		printSeparator("Starting");
		final HBaseClient hbc = new HBaseClient();
		hbc.listTables();

		// hbc.scanData("Order");
		// hbc.deleteTable("employe");
		// hbc.createTable("employe", new String[] { "personal", "professional" });
		// hbc.listColumn("employe");

		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new
		// Tuple<String>("personal", "city"));
		// hbc.insertData("employe", "row1", new Triple<String>("personal", "name",
		// "raju"),
		// new Triple<String>("personal", "city", "hyderabad"),
		// new Triple<String>("professional", "designation", "manager"),
		// new Triple<String>("professional", "salary", "50000"));

		// hbc.scanData("employe");
		// hbc.scanData("employe", new Tuple<String>("personal", "name"), new
		// Tuple<String>("professional", "salary"));

		// hbc.updateData("employe", "row1", new Triple<String>("personal", "city",
		// "helloya"));
		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new
		// Tuple<String>("personal", "city"));

		// hbc.deleteRow("employe", "row1");
		// hbc.getData("employe", "row1", new Tuple<String>("personal", "name"), new
		// Tuple<String>("personal", "city"));

		// hbc.insertDatasets();

		hbc.query7();
		hbc.query8("UK_Gear", 2023);

		printSeparator("Exiting");
	}

	public final void insertDatasets() throws IOException {
		final String[] xmlColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		final String[] xmlsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		final String xmlfilepath = getClass().getClassLoader().getResource("Invoice.xml").getFile();
		insertXML("Invoice", xmlfilepath, xmlColnames, xmlsub_orderLineColnames);

		String csvfilepath = getClass().getClassLoader().getResource("person_0_0.csv").getFile();
		deleteTable("Person");
		insertCSV("Person", csvfilepath, "\\|");

		csvfilepath = getClass().getClassLoader().getResource("Product.csv").getFile();
		deleteTable("Product");
		insertCSV("Product", csvfilepath);

		csvfilepath = getClass().getClassLoader().getResource("BrandByProduct.csv").getFile();
		deleteTable("BrandByProduct");
		insertCSV("BrandByProduct", csvfilepath, new String[] { "brand", "asin" });

		csvfilepath = getClass().getClassLoader().getResource("Feedback.csv").getFile();
		deleteTable("Feedback");
		insertCSV("Feedback", csvfilepath, "\\|", new String[] { "asin", "PersonId", "feedback" });

		csvfilepath = getClass().getClassLoader().getResource("Vendor.csv").getFile();
		deleteTable("Vendor");
		insertCSV("Vendor", csvfilepath);

		final String[] jsonColnames = new String[] { "OrderId", "PersonId", "OrderDate", "TotalPrice", "Orderline" };
		final String[] jsonsub_orderLineColnames = new String[] { "productId", "asin", "title", "price", "brand" };
		final String filepathjson = getClass().getClassLoader().getResource("Order.json").getFile();
		insertJSON("Order", filepathjson, jsonColnames, jsonsub_orderLineColnames);
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
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				print("Found row :", result);
				for (final Tuple<String> st : sts)
					print(st.a, "-", st.b, ":", Bytes.toString(result.getValue(st.a.getBytes(), st.b.getBytes())));
			}
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
		for (int index = 1; index < size; index += 2) {
			final Node node = rootNodes.item(index);
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
			// TODO REMOVEME
			if (index > 2 * insertedLineMax)
				break;
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
			for (int index = 0; (ligne = br.readLine()) != null; index++) {
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
				// TODO REMOVEME
				if (index > insertedLineMax)
					break;
			}
			br.close();
		} catch (final Exception e) {
			print(e.toString());
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
			for (int index = 0; (line = br.readLine()) != null; index++) {
				final String[] data = line.split(separator);
				final Put p = new Put(Integer.toString(index).getBytes());
				for (byte j = 0; j < headers.length; j++)
					p.addColumn(headers[j].getBytes(), headers[j].getBytes(), data[j].getBytes());
				table.put(p);
				// TODO REMOVEME
				if (index > insertedLineMax)
					break;
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
				// TODO REMOVEME
				if (index > insertedLineMax)
					break;
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
		table.close();
	}

	public final void query8(final String vendor, final int annee) throws IOException {
		printSeparator("QUERY 8");

		// On récupère les produits d'une marque
		Table table = conn.getTable(TableName.valueOf("BrandByProduct"));
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter("brand".getBytes(), "brand".getBytes(), CompareOp.EQUAL,
				new BinaryComparator(vendor.getBytes())));
		ResultScanner rScanner = table.getScanner(scan);
		List<byte[]> ids = new LinkedList<byte[]>();
		for (final Result res : rScanner) {
			byte[] productId = res.getValue("asin".getBytes(), "asin".getBytes());
			ids.add(productId);
		}

		// On récupère les commandes passés avec les produits de la marque sur une année
		// donnée
		table.close();
		table = conn.getTable(TableName.valueOf("Order"));
		scan = new Scan();
		List<Filter> fl = new LinkedList<Filter>();
		fl.add(new SingleColumnValueFilter("OrderDate".getBytes(), "OrderDate".getBytes(), CompareOp.EQUAL,
				new SubstringComparator(Integer.toString(annee))));
		List<byte[]> idVendu = new LinkedList<byte[]>();
		for (final byte[] id : ids)
			fl.add(new SingleColumnValueFilter("Orderline".getBytes(), id, CompareOp.EQUAL,
					new BinaryComparator(vendor.getBytes())));

		scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, fl));
		rScanner = table.getScanner(scan);

		// Calcule du total des ventes d'une année donnée
		Double total_vente = 0.0;
		for (final Result res : rScanner) {
			idVendu.add(res.getRow());
			total_vente += Double.parseDouble(Bytes.toString(res.getValue("Orderline".getBytes(), "price".getBytes())));
		}

		print("La marque", vendor, "a un montant de vente de ", df.format(total_vente), "euros en ", annee);

		// On récupère les feedback sur les produits achetés à une année donnée
		table.close();
		table = conn.getTable(TableName.valueOf("Feedback"));
		scan = new Scan();
		fl = new LinkedList<Filter>();
		for (final byte[] id : idVendu)
			fl.add(new SingleColumnValueFilter("asin".getBytes(), id, CompareOp.EQUAL,
					new BinaryComparator(vendor.getBytes())));

		scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, fl));
		Map<String, List<Integer>> productByNote = new HashMap<String, List<Integer>>();
		rScanner = table.getScanner(scan);
		for (final Result res : rScanner) {
			final String productId = Bytes.toString(res.getValue("asin".getBytes(), "asin".getBytes()));
			final int note = Integer.parseInt(
					Bytes.toString(res.getValue("feedback".getBytes(), "feedback".getBytes())).substring(1, 2));
			if (!productByNote.containsKey(productId))
				productByNote.put(productId, new LinkedList<Integer>());
			productByNote.get(productId).add(note);
		}

		// Calcul des moyennes des notes par produit
		for (final String key : productByNote.keySet()) {
			final List<Integer> lp = productByNote.get(key);
			final int moy = lp.stream().reduce(0, Integer::sum);
			print("Le produit", key, "a en moyenne", df.format((double) moy / (double) lp.size()), "/5");
		}
		table.close();
	}

	public final void query7() throws IOException {
		printSeparator("QUERY 7");

		Table table = conn.getTable(TableName.valueOf("Vendor"));
		Scan scan = new Scan();
		ResultScanner rScanner = table.getScanner(scan);
		List<oTuple<String, Double>> vendors = new LinkedList<oTuple<String, Double>>();
		for (final Result res : rScanner)
			vendors.add(new oTuple<String, Double>(Bytes.toString(res.getRow()), Double.MAX_VALUE));
		table.close();

		for (final oTuple<String, Double> ot : vendors) {
			final String vendor = ot.a;
			final String[] annees = new String[] { "2018", "2019", "2020", "2021", "2022", "2023", "2024" };

			for (final String annee : annees) {
				// On récupère les produits d'une marque
				table = conn.getTable(TableName.valueOf("BrandByProduct"));
				scan = new Scan();
				scan.setFilter(new SingleColumnValueFilter("brand".getBytes(), "brand".getBytes(), CompareOp.EQUAL,
						new BinaryComparator(vendor.getBytes())));
				rScanner = table.getScanner(scan);
				List<byte[]> ids = new LinkedList<byte[]>();
				for (final Result res : rScanner) {
					byte[] productId = res.getValue("asin".getBytes(), "asin".getBytes());
					ids.add(productId);
				}

				// On récupère les commandes passés avec les produits de la marque sur une année
				// donnée
				table.close();
				table = conn.getTable(TableName.valueOf("Order"));
				scan = new Scan();
				List<Filter> fl = new LinkedList<Filter>();
				fl.add(new SingleColumnValueFilter("OrderDate".getBytes(), "OrderDate".getBytes(), CompareOp.EQUAL,
						new SubstringComparator(annee)));
				List<byte[]> idVendu = new LinkedList<byte[]>();
				for (final byte[] id : ids)
					fl.add(new SingleColumnValueFilter("Orderline".getBytes(), id, CompareOp.EQUAL,
							new BinaryComparator(vendor.getBytes())));

				scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, fl));
				rScanner = table.getScanner(scan);

				// Calcule du total des ventes d'une année donnée
				Double total_vente = 0.0;
				for (final Result res : rScanner) {
					idVendu.add(res.getRow());
					total_vente += Double
							.parseDouble(Bytes.toString(res.getValue("Orderline".getBytes(), "price".getBytes())));
				}

				if (ot.b - total_vente < 0)
					print("La marque", vendor, "a moins vendu que le trimestre précédent", df.format(ot.b), "pour",
							df.format(total_vente), "euros en ", annee);

				ot.b = total_vente;
				table.close();
			}
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
