package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {

	private SQLiteDatabase db;
	static final String TAG = "SDY";
	private static final String DATABASE_NAME = "DSSpring2016";
	private static final String KEY_VALUE_TABLE = "KeyValueTable";
	private static final String KEY_COLUMN_NAME = "key";
	private static final String VALUE_COLUMN_NAME = "value";
	private static final String NODE_COLUMN_NAME = "node";
	private static final String VERSION_COLUMN_NAME = "version";
	private static final int DATABASE_VERSION = 1;
	private static final String CREATE_TABLE_SQL_QUERY = "CREATE TABLE " + KEY_VALUE_TABLE +
			"( "+ KEY_COLUMN_NAME +" TEXT PRIMARY KEY NOT NULL, " + VALUE_COLUMN_NAME +" TEXT, " +
			NODE_COLUMN_NAME + " TEXT NOT NULL, "+ VERSION_COLUMN_NAME +" INT NOT NULL);";
	static String myPort;
	static String myHash;
	final int SERVER_PORT = 10000;
	final static String[] ports = { "5554", "5556", "5558", "5560", "5562" };
	private static final int QUORUM_SIZE = 3;//ports.length/2 + 1;
	static String ENTIRE_RING = "*";
	static String LOCAL_NODE = "@";
	TreeMap<String, String> nodes = new TreeMap<String, String>();
	AtomicInteger messageIDGenerator = new AtomicInteger();
	Lock dbLock = new ReentrantLock();
	ExecutorService executorService = Executors.newFixedThreadPool(16);

	PriorityBlockingQueue<Message> requestQueue  = new PriorityBlockingQueue<Message>();
	PriorityBlockingQueue<Message> messageOutQueue = new PriorityBlockingQueue<Message>();
	ConcurrentMap<Integer, CountDownLatch> operationsTimers = new ConcurrentHashMap<Integer, CountDownLatch>();
	ConcurrentMap<Integer, List<Message>> requestResponses = new ConcurrentHashMap<Integer, List<Message>>();
	boolean isSyncDone 	= false;

	private static class DatabaseHelper extends SQLiteOpenHelper {

		DatabaseHelper(Context context){
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}
		/**
		 * Called when the database is created for the first time. This is where the
		 * creation of tables and the initial population of the tables should happen.
		 *
		 * @param db The database.
		 */
		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(CREATE_TABLE_SQL_QUERY);
		}

		/**
		 * Called when the database needs to be upgraded. The implementation
		 * should use this method to drop tables, add tables, or do anything else it
		 * needs to upgrade to the new schema version.
		 * <p>
		 * <p>
		 * The SQLite ALTER TABLE documentation can be found
		 * <a href="http://sqlite.org/lang_altertable.html">here</a>. If you add new columns
		 * you can use ALTER TABLE to insert them into a live table. If you rename or remove columns
		 * you can use ALTER TABLE to rename the old table, then create the new table and then
		 * populate the new table with the contents of the old table.
		 * </p><p>
		 * This method executes within a transaction.  If an exception is thrown, all changes
		 * will automatically be rolled back.
		 * </p>
		 *
		 * @param db         The database.
		 * @param oldVersion The old database version.
		 * @param newVersion The new database version.
		 */
		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " + KEY_VALUE_TABLE);
			onCreate(db);
		}
	}

	@Override
	public boolean onCreate() {
		DatabaseHelper databaseHelper = new DatabaseHelper(getContext());
		db = databaseHelper.getWritableDatabase();
		try{
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		}catch(NullPointerException e){
			Log.e(TAG, "Unable to get port number: ", e );
		}
		myHash = genHash(myPort);
		for(String port: ports){
			nodes.put(genHash(port), port);
		}
		Log.i(TAG,"=======================Start:"+myPort+"=======================");
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Thread requestHandler = new RequestHandler();
			requestHandler.start();
			Thread messageSender = new MessageSender();
			messageSender.start();
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket: ", e);
		}
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				performSyncOperation();
			}
		});
		return !(db == null);
	}

	private void performSyncOperation(){
		List<String> myNodes = getNodesForPort(myPort);
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Sync);
		msg.data = myNodes;
		int i = 0 ;
		String[] outPorts = new String[ports.length-1];
		for(String port: ports){
			if(!myPort.equals(port)){
				outPorts[i++] = port;
			}
		}
		Log.i(TAG,"Sync:I " + Arrays.toString(outPorts) + " ,m: " + msg);
		addMessagesToOutQueue(msg, outPorts);
		try {
			operationsTimers.get(msg.messageId).await(3000,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Log.e(TAG,"Sync: Ex: ", e);
		}
		Log.i(TAG,"Sync: responses: " + requestResponses.get(msg.messageId).size());
		Map<String, String> keyValues = new HashMap<String, String>();
		Map<String, Integer> keyVersions = new HashMap<String, Integer>();
		for(Message message : requestResponses.get(msg.messageId)) {
			if (message == null || message.data == null || !(message.data instanceof Set)) {
				continue;
			}
			Set<Row> data = (Set<Row>) message.data;
			for (Row row: data) {
				keyVersions.put(row.key, row.version);
				keyValues.put(row.key, row.value);
			}
		}
		operationsTimers.remove(msg.messageId);
		requestResponses.remove(msg.messageId);
		ContentValues values = new ContentValues();
		try{
			dbLock.lock();
			db.beginTransaction();
			for(Map.Entry<String, String> entry : keyValues.entrySet()){
				values.put(KEY_COLUMN_NAME, entry.getKey());
				values.put(VALUE_COLUMN_NAME, entry.getValue());
				values.put(VERSION_COLUMN_NAME, keyVersions.get(entry.getKey()));
				values.put(NODE_COLUMN_NAME, nodes.get(getHashNodeForHash(genHash(entry.getKey()))));
		//		Log.i(TAG,"Syin: " + values);
				db.replace(KEY_VALUE_TABLE, null, values);
			}
		}catch (Exception e){
			Log.e(TAG, "Sync: Ex: ", e);
		}finally {
			db.setTransactionSuccessful();
			db.endTransaction();
			dbLock.unlock();
		}
		isSyncDone = true;
		Log.i(TAG,"Sync: Done");
	}

	private class RequestHandler extends Thread{

		@Override
		public void run() {
			while (true){
				if(!isSyncDone && !requestQueue.isEmpty() && !(requestQueue.peek().mType == MessageType.Sync || requestQueue.peek().mType == MessageType.SyncResponse)){
					continue;
				}else{
					final Message message ;
					try{
						message = requestQueue.take();
					}catch (Exception e){
						Log.e(TAG,"Ex: " ,e);
						continue;
					}

					executorService.execute(new Runnable() {
						@Override
						public void run() {
								handleMessage(message);
						}
					});
				}
			}
		}
	}

	private void handleMessage(Message message){
		if(message == null){
			return;
		}
		switch (message.mType){
			case Sync:
				handleSyncRequest(message);
				break;
			case Insert:
				handleInsert(message);
				break;
			case Delete:
				handleDelete(message);
				break;
			case Query:
				handleQuery(message);
				break;
			case AllQuery:
				handleAllQuery(message);
				break;
			case AllDelete:
				handleAllDelete(message);
				break;
			case AllQueryResponse:
			case InsertResponse:
			case DeleteResponse:
			case QueryResponse:
			case SyncResponse:
				handleResponse(message);
				break;
			default:
				Log.i(TAG,"Unknown message Type: " + message);
		}
	}
	private void handleAllQuery(Message message){
		//Log.i(TAG,"HAQ: " + message);
		Cursor cursor = localQueryAll();
		message.data = extractRowsFromCursor(cursor);
		message.receiverId = message.senderId;
		message.mType = MessageType.AllQueryResponse;
		//Log.i(TAG,"HAQE: " + message);
		messageOutQueue.add(message);
	}

	private void handleInsert(Message message) {
		Log.i(TAG,"HI: " + message);
		Row row = (Row) message.data;
		String key = row.key;
		String value = row.value;
		message.receiverId = message.senderId;
		message.mType  = MessageType.InsertResponse;
		long rowId = localInsert(key,value, nodes.get(getHashNodeForHash(genHash(key))));
		message.data = rowId;
		Log.i(TAG,"HI: E: " + message);
		messageOutQueue.add(message);
	}

	private void handleResponse(Message message){
		//Log.i(TAG,"HR: " + message);
		if(requestResponses.containsKey(message.messageId)){
			requestResponses.get(message.messageId).add(message);
		}
		if(operationsTimers.containsKey(message.messageId)){
			operationsTimers.get(message.messageId).countDown();
		}
	}

	private void handleDelete(Message message) {
		Row row = (Row) message.data;
		int count = localQuery(row.key).getCount();
		//Log.i(TAG,"HD: " + message);
		message.data = count;
		localInsert(row.key, null, getHashNodeForHash(genHash(row.key)));
		message.receiverId = message.senderId;
		message.mType = MessageType.DeleteResponse;
		//Log.i(TAG,"HDE: " + message);
		messageOutQueue.add(message);
	}

	private void handleQuery(Message message) {
		//Log.i(TAG,"HQ: " + message);
		Row row = (Row) message.data;
		String key = row.key;
		Cursor cursor = localQuery(key);
		message.data = null;
		Set<Row> rowSet = extractRowsFromCursor(cursor);
		if(rowSet.size() > 0){
			message.data = rowSet.iterator().next();
		}
		message.mType = MessageType.QueryResponse;
		message.receiverId = message.senderId;
		Log.i(TAG,"HQ:R: " + message);
		messageOutQueue.add(message);
	}

	private void handleAllDelete(Message message) {
		Log.i(TAG,"HandleAllDelete: " + message);
	}

	private String getPreviousForHash(String hash){
		if(hash.equals(nodes.firstKey())){
			return nodes.lastKey();
		}else{
			String prev = null;
			for(String key: nodes.keySet()){
				if(key.equals(hash)){
					return prev;
				}
				prev = key;
			}
		}
		return null;
	}

	private List<String> getNodesForPort(String port){
		String[] portNodes = new String[QUORUM_SIZE];
		portNodes[0] = getHashNodeForHash(genHash(port));
		for(int i = 1; i < QUORUM_SIZE; i++){
			portNodes[i] = getPreviousForHash(portNodes[i-1]);
		}
		//Log.i(TAG,"portHashes: " + Arrays.toString(portNodes));
		for(int i=0; i < portNodes.length; i++){
			portNodes[i] = nodes.get(portNodes[i]);
		}
		//Log.i(TAG,"PortNodes : " + Arrays.toString(portNodes));
		return Arrays.asList(portNodes);
	}

	private void handleSyncRequest(Message message){
		List<String> nodesForPort = (List<String>) message.data;
		Log.i(TAG,"Sync:" + nodesForPort + ", m:" + message );
		Cursor cursor = localQueryAll();
		message.data = null;
		Set<Row> data = new HashSet<Row>();
		int kIndex = cursor.getColumnIndex(KEY_COLUMN_NAME);
		int vIndex = cursor.getColumnIndex(VALUE_COLUMN_NAME);
		int nIndex = cursor.getColumnIndex(NODE_COLUMN_NAME);
		int rIndex = cursor.getColumnIndex(VERSION_COLUMN_NAME);
		for(cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()){
			Row row = new Row();
			row.key = cursor.getString(kIndex);
			row.value = cursor.getString(vIndex);
			row.version = cursor.getInt(rIndex);
			//Log.i(TAG,"S:row: " + row + " ,n: " + cursor.getString(nIndex));
			if(nodesForPort.contains(cursor.getString(nIndex))){
				data.add(row);
			}
		}
		message.data = data;
		message.receiverId = message.senderId;
		message.mType = MessageType.SyncResponse;
		//Log.i(TAG,"SR: " + data);
		messageOutQueue.add(message);
	}

	private class MessageSender extends Thread{
		@Override
		public void run() {
			while (true){
				try {
					while (!messageOutQueue.isEmpty()) {
						Message msg = messageOutQueue.take();
						if(myPort.equals(msg.receiverId)){
							requestQueue.add(msg);
						}else{
							sendMessage(msg);
						}
					}
				}catch (Exception e){
					Log.e(TAG,"Exception in Req Q: ", e);
				}
			}
		}

		private void sendMessage(Message msg){
			try{
				int port = Integer.parseInt(msg.receiverId) * 2;
				if(msg.mType == MessageType.Sync || msg.mType == MessageType.SyncResponse){
					Log.i(TAG,"Sync: port: " + port + " msg: " + msg);
				}
				Socket socket = new  Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msg);
				out.flush();
				out.close();
				socket.close();
			}catch (Exception e){
//				if(msg.mType == MessageType.Sync){
//					operationsTimers.get(msg.messageId).countDown();
//				}
				Log.e(TAG, "SM:Exception " + msg + " , " + e.getMessage());
			}
		}
	}

	public String getReplicaForHash(String hash){
		String prev = null;
		for(String key: nodes.keySet()){
			if(hash.equals(prev)){
				return key;
			}
			prev = key;
		}
		return nodes.firstKey();
	}

	public String getHashNodeForHash(String hash){
		String response = nodes.ceilingKey(hash);
		if(response == null){
			return nodes.firstKey();
		}
		return response;
	}

	public synchronized String[] getNodeHashForKey(String key){
		String[] outNodes = new String[QUORUM_SIZE];
		outNodes[0] = getHashNodeForHash(genHash(key));
		for(int i = 1; i < QUORUM_SIZE; i++){
			outNodes[i] = getReplicaForHash(outNodes[i-1]);
		}
		for(int i=0; i < outNodes.length; i++){
			outNodes[i] = nodes.get(outNodes[i]);
		}
		return outNodes;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		//Log.i(TAG,"Delete: " + selection);
		return performDelete(selection);
	}

	private synchronized void addMessagesToOutQueue(Message msg, String[] outPorts){
		operationsTimers.put(msg.messageId, new CountDownLatch(outPorts.length - 1));
		requestResponses.put(msg.messageId, new ArrayList<Message>());
		for(String port: outPorts){
			Message m1 = new Message(msg);
			m1.receiverId = port;
			messageOutQueue.add(m1);
		}
	}

	private int performDelete(String key){
		//TODO: This does not handle @ and * for now
		//TODO: check if they actually use the integer returned by the delete()
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Delete);
		msg.data = new Row(key);
		addMessagesToOutQueue(msg, getNodeHashForKey(key));
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"IN:Exception: ",e);
		}
		int rowCount = Integer.MIN_VALUE;
		for(Message message: requestResponses.get(msg.messageId)){
			if(message != null && !(message.data instanceof  Integer)){
				rowCount = Math.max(rowCount, Integer.parseInt(message.data.toString()));
			}
		}
		requestResponses.remove(msg.messageId);
		operationsTimers.remove(msg.messageId);
		return 0; // TODO: experimenting if they are checking count , could save computation
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.i(TAG,"IN: " + values.toString());
		performInsert(values.getAsString(KEY_COLUMN_NAME), values.getAsString(VALUE_COLUMN_NAME));
		return null;
	}

	private long localInsert(String key, String value, String node){
		//TODO: this does not handle versioning , need to implement it later
		ContentValues values = new ContentValues();
		values.put(KEY_COLUMN_NAME, key);
		values.put(VALUE_COLUMN_NAME, value);
		values.put(NODE_COLUMN_NAME, node);
		values.put(VERSION_COLUMN_NAME,1);
		long rowId = -1;
		try{
			dbLock.lock();
			Cursor cursor = localQuery(key);
			Set<Row> rows = extractRowsFromCursor(cursor);
			if(rows.size() > 0 ){
				cursor.moveToFirst();
				int version = cursor.getInt(cursor.getColumnIndex(VERSION_COLUMN_NAME));
				values.put(VERSION_COLUMN_NAME,version+1);
			}
			rowId = db.replace(KEY_VALUE_TABLE, "", values);
		}catch (Exception e){
			Log.e(TAG,"Exception: Insert: " + values, e);
		}finally{
			dbLock.unlock();
		}
		return rowId;
	}

	private Cursor localQuery(String key){
		Cursor cursor ;
		try{
			dbLock.lock();
			cursor = db.query(KEY_VALUE_TABLE, null, KEY_COLUMN_NAME + "=?", new String[]{key}, null, null, null);
		}finally {
			dbLock.unlock();
		}
		return cursor;
	}

	private long performInsert(String key, String value){
		String[] outNodes = getNodeHashForKey(key);
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Insert);
		msg.data =new Row(key, value);
		Log.i(TAG,"IN: " + Arrays.toString(outNodes) + " ,m: " + msg);
		addMessagesToOutQueue(msg, outNodes);
		try{
			operationsTimers.get(msg.messageId).await();
			requestResponses.remove(msg.messageId);
			operationsTimers.remove(msg.messageId);
		} catch (InterruptedException e) {
			Log.e(TAG,"IN:Exception: ",e);
		}
		Log.i(TAG,"IN:Done: " + Arrays.toString(outNodes) + " ,m: " + msg);
		return 1;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.i(TAG,"Query: " + selection);
		return performQuery(selection);
	}

	private Cursor performQuery(String key) {
		//TODO: no * and @
		if(key.equals(LOCAL_NODE)){
			return performLocalGetAll();
		}
		if(key.equals(ENTIRE_RING)){
			return performQueryRing();
		}
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Query);
		msg.data = new Row(key);
		Log.i(TAG,"PQ: " + msg +" ,n: " + Arrays.toString(getNodeHashForKey(key)));
		addMessagesToOutQueue(msg, getNodeHashForKey(key));
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"IN:Exception: ",e);
		}
		int version = Integer.MIN_VALUE;
		String value = null;
		for(Message message : requestResponses.get(msg.messageId)){
			if(message == null || message.data == null || !(message.data instanceof Row)){
				continue;
			}
			Row row = (Row)message.data;
			if(row.value != null){
				if(row.version > version){
					value = row.value;
					version = row.version;
				}
			}
		}
		requestResponses.remove(msg.messageId);
		operationsTimers.remove(msg.messageId);
		Log.i(TAG,"PQE: " + key + ",v: " + value + ", Vr: " + version);
		MatrixCursor cursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});
		if(value != null){
			cursor.addRow(new Object[]{key, value});
		}
		return cursor;
	}

	private Cursor localQueryAll(){
		Cursor cursor;
		try{
			dbLock.lock();
			cursor = db.query(KEY_VALUE_TABLE, null, null, null, null, null, null);
		}finally{
			dbLock.unlock();
		}
		return cursor;
	}

	private synchronized Cursor rowsToCursor(int messageId){
		Map<String, Integer> keyVersions = new HashMap<String, Integer>();
		Map<String , String> keyValues = new HashMap<String, String>();
		for(Message message : requestResponses.get(messageId)){
			if(message == null || message.data == null){
				continue;
			}
			Set<Row> rows = (Set<Row>) message.data;
			for(Row row: rows){
				if(keyVersions.containsKey(row.key)){
					if(row.version > keyVersions.get(row.key)){
						keyValues.put(row.key,row.value);
						keyVersions.put(row.key,row.version);
					}
				}else{
					keyValues.put(row.key,row.value);
					keyVersions.put(row.key,row.version);
				}
			}
		}
		MatrixCursor cursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME, VALUE_COLUMN_NAME});
		for(Map.Entry<String, String> entry: keyValues.entrySet()){
			if(entry.getValue() != null){
				cursor.addRow(new Object[]{entry.getKey(), entry.getValue()});
			}
		}
		requestResponses.remove(messageId);
		operationsTimers.remove(messageId);
		Log.i(TAG,"PLQE: " + keyValues);
		return cursor;
	}

	private Cursor performLocalGetAll(){
		Cursor cursor;
		try{
			dbLock.lock();
			 cursor = db.rawQuery("SELECT key,value from KeyValueTable WHERE value IS NOT NULL", null);
		}finally {
			dbLock.unlock();
		}

//		for(cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()){
//			DatabaseUtils.dumpCurrentRow(cursor);
//		}
//		cursor.moveToFirst();
		return cursor;
	}

	private Cursor performQueryRing(){
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.AllQuery);
		addMessagesToOutQueue(msg, ports);
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"IN:Exception: ",e);
		}
		return rowsToCursor(msg.messageId);
	}

	private Set<Row> extractRowsFromCursor(Cursor cursor){
		Set<Row> values = new HashSet<Row>();
		for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()){
			Row row = new Row(cursor.getString(cursor.getColumnIndex(KEY_COLUMN_NAME)));
			row.version = cursor.getInt(cursor.getColumnIndex(VERSION_COLUMN_NAME));
			row.value = cursor.getString(cursor.getColumnIndex(VALUE_COLUMN_NAME));
			values.add(row);
		}
		return values;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while(true){
				try {
					Socket socket =  serverSocket.accept();
					ObjectInputStream input  = new ObjectInputStream(socket.getInputStream());
					Message msgRecieved = (Message) input.readObject();
					Log.i(TAG, "SERVER: msg rec: " + msgRecieved);
					requestQueue.add(msgRecieved);
					input.close();
					socket.close();
				} catch (Exception e) {
					Log.e(TAG, "Exception in ServerSocket: ", e);
				}
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private String genHash(String input){
		try{
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			byte[] sha1Hash = sha1.digest(input.getBytes());
			Formatter formatter = new Formatter();
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
			return formatter.toString();
		}catch( NoSuchAlgorithmException e){
			Log.e(TAG,"Exception: ", e);
		}
		return "";
	}
}
