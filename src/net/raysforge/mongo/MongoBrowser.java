package net.raysforge.mongo;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import javax.swing.JMenu;
import javax.swing.JOptionPane;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import net.raysforge.commons.Generics;
import net.raysforge.easyswing.EasySplitPane;
import net.raysforge.easyswing.EasySwing;
import net.raysforge.easyswing.EasyTable;
import net.raysforge.easyswing.EasyTree;

import org.bson.types.ObjectId;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class MongoBrowser implements TreeSelectionListener, ActionListener {

	private Mongo mongo;
	private EasyTree easyTree;
	private EasySplitPane esp;
	private EasySwing es;
	private DefaultMutableTreeNode rootNode;
	private EasyTable easyTable;

	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	JsonParser jp = new JsonParser();

	public MongoBrowser() throws UnknownHostException, MongoException {

		Object[] vs = { "", "178.202.136.198", "192.168.178.23", "oldshatterhand", "10.0.0.71", "ds029837.mongolab.com" };
		Object server = JOptionPane.showInputDialog(null, "Select Server", "Server", JOptionPane.QUESTION_MESSAGE, null, vs, vs[0]);
		mongo = new Mongo(server.toString(), server.toString().startsWith("ds0") ? 29837 : 27017);

		if (server.toString().startsWith("ds0") || server.toString().endsWith(".23")) {
			DB ww = mongo.getDB("admin");
			Object username = JOptionPane.showInputDialog(null, "username", "username", JOptionPane.QUESTION_MESSAGE);
			Object password = JOptionPane.showInputDialog(null, "password (cleartext)", "password", JOptionPane.QUESTION_MESSAGE);

			ww.authenticate(""+username, (""+password).toCharArray());
		}

		es = new EasySwing("MongoBrowser", 800, 600);
		es.addToolBarItem("Reload", "reload", this);
		es.addToolBarItem("Purge DB", "purgeDB", this);
		es.addToolBarItem("KillOp", "killOp", this);
		es.addToolBarItem("Download", "download", this);

		JMenu fileMenuItem = es.addMenuItem("File");

		es.addMenuItem(fileMenuItem, "Purge DB", "purgeDB", this);
		es.addMenuItem(fileMenuItem, "KillOp", "killOp", this);

		esp = es.setSplitPaneAsMainContent(false, 100);
		esp.getSplitPane().setDividerLocation(200);

		easyTree = new EasyTree("MongoDB");
		easyTree.addTreeSelectionListener(this);

		esp.setLeft(easyTree.getScrollPane());

		easyTable = new EasyTable();
		esp.setRight(easyTable);

		rootNode = easyTree.getRootNode();

		loadNodes();

		es.show();
	}

	private void loadNodes() {
		List<String> databaseNames = mongo.getDatabaseNames();
		rootNode.removeAllChildren();

		DefaultMutableTreeNode sysNode = new DefaultMutableTreeNode("_system");
		rootNode.add(sysNode);

		DefaultMutableTreeNode opNode = new DefaultMutableTreeNode("_currentOp");
		sysNode.add(opNode);

		BasicDBList basicDBList = getCurrentOp();
		int size = basicDBList.size();
		for (int i = 0; i < size; i++) {
			BasicDBObject dbObject = (BasicDBObject) basicDBList.get(i);
			DefaultMutableTreeNode opid = new DefaultMutableTreeNode(dbObject.get("opid"));
			opNode.add(opid);
		}

		for (String dn : databaseNames) {
			DefaultMutableTreeNode dnNode = new DefaultMutableTreeNode(dn);
			rootNode.add(dnNode);
			DB db = mongo.getDB(dn);
			Set<String> collectionNames = db.getCollectionNames();
			for (String cn : collectionNames) {
				DefaultMutableTreeNode cnNode = new DefaultMutableTreeNode(cn + " " + db.getCollection(cn).count());
				dnNode.add(cnNode);
			}
		}
		easyTree.getModel().reload();
	}

	private BasicDBList getCurrentOp() {
		CommandResult command = (CommandResult) eval("db.currentOp()");
		BasicDBList basicDBList = (BasicDBList) command.get("inprog");
		return basicDBList;
	}

	@Override
	public void valueChanged(TreeSelectionEvent e) {
		try {
			Object[] path = e.getPath().getPath();
			if (path.length == 1)
				return;

			StringBuffer str = new StringBuffer();

			//easyTable.clear();

			String pathName = path[1].toString();
			if (path.length != 4 && pathName.startsWith("_")) {
				return;
			}

			if (path.length == 1) {
			}

			if (path.length == 2) {
				DB db = mongo.getDB(pathName);
				dbStats(db, str);
			}

			if (path.length == 3) {
				System.out.println(path[1] + "." + path[2]);

				DB db = mongo.getDB(pathName);
				DBCollection coll = db.getCollection(path[2].toString().substring(0, path[2].toString().lastIndexOf(' ')));

				getData(str, coll);
			}

			if (path.length == 4) {
				collectCurrentOp(path, str, pathName);
			}

			System.out.println(str);

		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

	private void collectCurrentOp(Object[] path, StringBuffer str, String pathName) {
		if (!pathName.startsWith("_")) {
			return;
		}
		BasicDBList basicDBList = getCurrentOp();
		int size = basicDBList.size();
		for (int i = 0; i < size; i++) {
			BasicDBObject dbObject = (BasicDBObject) basicDBList.get(i);
			System.out.println(dbObject.get("opid"));
			if (path[3].toString().equals(dbObject.get("opid").toString())) {
				str.append(jsonFormat(dbObject.toString()));
			}

		}
	}

	private void getData(StringBuffer str, DBCollection coll) {

		List<String> fn2cp = Generics.newArrayList(); // field name to column position

		DBCursor find = coll.find().limit(100); // new BasicDBObject("a", new BasicDBObject("$exists", true))

		int row = 0;

		easyTable.removeAllColumns();
		boolean once = true;

		while (find.hasNext()) {
			DBObject dbObject = find.next();
			if (coll.getName().equals("MMessage"))
				dbObject.removeField("b");

			Set<String> keySet = dbObject.keySet();
			if (once) {
				for (String key : keySet) {
					if (!fn2cp.contains(key)) {
						fn2cp.add(key);
					}
				}
				for (String colName : fn2cp) {
					easyTable.addColumn(colName);
				}
				once = false;
			}

			for (String key : keySet) {
				if (!fn2cp.contains(key)) {
					fn2cp.add(key);
					easyTable.addColumn(key);
				}
				easyTable.setValue(dbObject.get(key), row, fn2cp.indexOf(key));

			}

			row++;

			//			JsonElement je = jp.parse(string);
			//			String jsonOutput = gson.toJson(je);
			//			str.append(jsonOutput + "\n\n");
		}
	}

	private void dbStats(DB db, StringBuffer str) {

		// http://www.mongodb.org/display/DOCS/serverStatus+Command

		BasicDBObject dbStats = new BasicDBObject("dbStats", "");
		dbStats.put("scale", 1024 * 1024 * 1024);

		BasicDBObject[] cmds = { dbStats };

		for (BasicDBObject cmd : cmds) {
			CommandResult command = db.command(cmd);
			String jsonOutput = jsonFormat(command);
			str.append(jsonOutput);
		}

		Set<String> collectionNames = db.getCollectionNames();
		for (String cn : collectionNames) {
			BasicDBObject basicDBObject = new BasicDBObject("collstats", cn);
			basicDBObject.put("scale", 1024 * 1024 * 1024);
			CommandResult command = db.command(basicDBObject);
			String jsonOutput = jsonFormat(command);
			str.append(jsonOutput);
		}
	}

	private String jsonFormat(Object command) {
		JsonElement je = jp.parse(command.toString());
		String jsonOutput = gson.toJson(je);
		return jsonOutput;
	}

	public static void main(String[] args) throws UnknownHostException, MongoException {
		new MongoBrowser();

	}

	@Override
	public void actionPerformed(ActionEvent e) {
		if (e.getActionCommand().equals("reload")) {
			loadNodes();
		} else if (e.getActionCommand().equals("killOp")) {
			Object opid = easyTree.getJTree().getSelectionPath().getPathComponent(3);
			eval("db.killOp(" + opid + ")");
		} else if (e.getActionCommand().equals("download")) {
			Object pc = easyTree.getJTree().getSelectionPath().getPathComponent(1);
			GridFS gridfs = new GridFS(mongo.getDB(pc.toString()));

			int selectedRow = easyTable.getSelectedRow();
			int columnIndex = easyTable.getColumnIndex("_id");
			String value = easyTable.getValue(selectedRow, columnIndex);
			System.out.println(value);

			GridFSDBFile gridFile = gridfs.find(new ObjectId(value));
			File fileFromFileSaveDialog = es.getFileFromFileSaveDialog("file.eml");
			if (fileFromFileSaveDialog != null) {
				try {
					gridFile.writeTo(fileFromFileSaveDialog);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		} else if (e.getActionCommand().equals("purgeDB")) {
			Object pc = easyTree.getJTree().getSelectionPath().getPathComponent(1);
			System.out.println(pc);
			mongo.dropDatabase(pc.toString());
			loadNodes();
			//easyTable.clear();
		}
	}

	public Object eval(String str) {
		return mongo.getDB(mongo.getDatabaseNames().get(0)).eval(str);
	}
}
