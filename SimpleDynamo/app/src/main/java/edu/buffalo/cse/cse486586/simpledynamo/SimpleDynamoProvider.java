package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
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

	AtomicInteger requestIdGenerator = new AtomicInteger(0);
	PriorityBlockingQueue<Message> requestQueue  = new PriorityBlockingQueue<Message>(100,new MessageComparator());
	ConcurrentLinkedQueue<Message> messageOutQueue = new ConcurrentLinkedQueue<Message>();
	ConcurrentMap<Integer, CountDownLatch> operationsTimers = new ConcurrentHashMap<Integer, CountDownLatch>();
	ConcurrentMap<Integer, ConcurrentLinkedQueue<Message>> requestResponses = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Message>>();
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

		Thread reSyncThread = new ReSyncKeys();
		reSyncThread.start();

		return !(db == null);
	}

	private class ReSyncKeys extends Thread{
		@Override
		public void run() {
			performSyncOperation();
		}
	}

	private class MessageComparator implements Comparator<Message>{
		@Override
		public int compare(Message lhs, Message rhs) {
			if(!isSyncDone){
				if(rhs == null){
					return -1;
				}
				if(lhs == null){
					return 1;
				}
				switch (lhs.mType){
					case Sync:
					case SyncResponse:
						return -1;
				}
				switch (rhs.mType){
					case Sync:
					case SyncResponse:
						return 1;
				}
			}
			return Integer.compare(lhs.requestId, rhs.requestId);
		}
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
		Log.i(TAG,"[Sync]: " + Arrays.toString(outPorts) + " ,msg: " + msg);
		addMessagesToOutQueue(msg, outPorts);
		try {
			Log.i(TAG, String.format("[Sync]:Wait:Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())));
			operationsTimers.get(msg.messageId).await(3000,TimeUnit.MILLISECONDS);
			Log.i(TAG, String.format("[Sync]:Wait:Done: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())));
		} catch (InterruptedException e) {
			Log.e(TAG,"[Sync]:Ex: ", e);
		}

		Map<String, String> keyValues = new HashMap<String, String>();
		Map<String, Integer> keyVersions = new HashMap<String, Integer>();
		ConcurrentLinkedQueue<Message> messages = requestResponses.get(msg.messageId);
		operationsTimers.remove(msg.messageId);
		requestResponses.remove(msg.messageId);
		Log.i(TAG,"[Sync]:Before: responses: " + messages.size());
		for(Message message : messages) {
			if (message == null || message.data == null || !(message.data instanceof Set)) {
				continue;
			}
			Set<Row> data = (Set<Row>) message.data;
			for (Row row: data) {
				if(!keyVersions.containsKey(row.key)){
					keyVersions.put(row.key, row.version);
					keyValues.put(row.key, row.value);
				}else{
					if(keyVersions.get(row.key) < row.version){
						keyVersions.put(row.key, row.version);
						keyValues.put(row.key, row.value);
					}
				}
			}
		}
		Log.i(TAG,"[Sync]:KeyVersions: " + keyVersions);
		Log.i(TAG,"[Sync]:KeyValues: " + keyValues);
		Set<Row> rows = extractRowsFromCursor(localQueryAll());
		for(Row row: rows){
			Log.i(TAG,"[Sync]:Local:Rows: " + row);
			if(keyVersions.containsKey(row.key)){
				if(keyVersions.get(row.key) < row.version){
					keyVersions.put(row.key, row.version);
					keyValues.put(row.key, row.value);
				}
			}
		}
		ContentValues values = new ContentValues();
		try{
			dbLock.lock();
			db.beginTransaction();
			for(Map.Entry<String, String> entry : keyValues.entrySet()){
				values.put(KEY_COLUMN_NAME, entry.getKey());
				values.put(VALUE_COLUMN_NAME, entry.getValue());
				values.put(VERSION_COLUMN_NAME, keyVersions.get(entry.getKey()));
				values.put(NODE_COLUMN_NAME, nodes.get(getHashNodeForHash(genHash(entry.getKey()))));
				Log.i(TAG,"[Sync]:insert: " + values);
				db.replace(KEY_VALUE_TABLE, null, values);
			}
		}catch (Exception e){
			Log.e(TAG, "[Sync]: Ex: ", e);
		}finally {
			db.setTransactionSuccessful();
			db.endTransaction();
			dbLock.unlock();
		}
		isSyncDone = true;
		Log.i(TAG,"[Sync]:Done");
	}

	private class RequestHandler extends Thread{

		@Override
		public void run() {
			while (true){
				while(!isSyncDone){
					if(!requestQueue.isEmpty() && (requestQueue.peek().mType == MessageType.Sync || requestQueue.peek().mType == MessageType.SyncResponse)){
						handleMessage(requestQueue.poll());
					}
				}
				if(!requestQueue.isEmpty()){
					handleMessage(requestQueue.poll());
				}
			}
		}
	}

	private void handleMessage(Message message){
		//Log.i(TAG,"[HandleMessage]: " + message);
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
				Log.i(TAG,"[HandleMessage]:Unknown Type: " + message);
		}
	}
	private void handleAllQuery(Message message){
		Log.i(TAG,"[Query]:H: " + message);
		Cursor cursor = localQueryAll();
		message.data = extractRowsFromCursor(cursor);
		message.receiverId = message.senderId;
		message.senderId = myPort;
		message.mType = MessageType.AllQueryResponse;
		Log.i(TAG,"[Query]:H " + message);
		messageOutQueue.add(message);
	}

	private void handleInsert(Message message) {
		Log.i(TAG,"[Insert]:H: " + message);
		Row row = (Row) message.data;
		String key = row.key;
		String value = row.value;
		message.receiverId = message.senderId;
		message.senderId = myPort;
		message.mType  = MessageType.InsertResponse;
		long rowId = localInsert(key,value, nodes.get(getHashNodeForHash(genHash(key))));
		message.data = rowId;
		Log.i(TAG,"[Insert]:H:Done: " + message);
		messageOutQueue.add(message);
	}

	private void handleResponse(Message message){
		Log.i(TAG,"[Response]:H: " + message);
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
		Log.i(TAG,"[Delete]:H:count: " + count +" ,msg:" + message );
		message.data = count;
		localInsert(row.key, null, nodes.get(getHashNodeForHash(genHash(row.key))));
		message.receiverId = message.senderId;
		message.senderId = myPort;
		message.mType = MessageType.DeleteResponse;
		Log.i(TAG,"[Delete]:H: " + message);
		messageOutQueue.add(message);
	}

	private void handleQuery(Message message) {
		Log.i(TAG,"[Query]:H: " + message);
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
		message.senderId = myPort;
		Log.i(TAG,"[Query]:H:Done " + message);
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
		Log.i(TAG,"[Sync]:H:" + nodesForPort + " ,msg:" + message );
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
			if(nodesForPort.contains(cursor.getString(nIndex))){
				data.add(row);
			}
		}
		message.data = data;
		message.receiverId = message.senderId;
		message.senderId = myPort;
		message.mType = MessageType.SyncResponse;
		Log.i(TAG,"[Sync]:H:Response: " + data);
		messageOutQueue.add(message);
	}

	private class MessageSender extends Thread{
		@Override
		public void run() {
			while (true){
				try {
					while (!messageOutQueue.isEmpty()) {
						final Message msg = messageOutQueue.poll();
						Log.i(TAG,"[MSend]: "+ msg);
						if(myPort.equals(msg.receiverId)){
							msg.requestId = requestIdGenerator.incrementAndGet();
							requestQueue.add(msg);
						}else{
							sendMessage(msg);
						}
					}
				}catch (Exception e){
					Log.e(TAG,"[MSend]:Exception: ", e);
				}
			}
		}

		private void sendMessage(Message msg){
			try{
				int port = Integer.parseInt(msg.receiverId) * 2;
				Log.i(TAG,"[MSend]: port: " + port + " msg: " + msg);
				Socket socket = new  Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msg);
				out.flush();
				out.close();
				socket.close();
			}catch (Exception e){
				Log.e(TAG, "[MSend]:Send:Exception " + msg, e);
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
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		Log.i(TAG,"[Query]: " + selection);
		Cursor cursor = performQuery(selection);
		Log.i(TAG,"[Query]:Done: " + selection + " ,cursor:  " + cursor.getCount());
		return cursor;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.i(TAG,"[Delete]: " + selection);
		int count = performDelete(selection);
		Log.i(TAG,"[Delete]:Done: " + selection + " ,count: " + count);
		return count;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.i(TAG,"[Insert]: " + values.toString());
		long row = performInsert(values.getAsString(KEY_COLUMN_NAME), values.getAsString(VALUE_COLUMN_NAME));
		Log.i(TAG,"[Insert]:Done: " + values.toString() + " Done:r: " + row);
		return null;
	}

	private synchronized void addMessagesToOutQueue(Message msg, String[] outPorts){
		operationsTimers.put(msg.messageId, new CountDownLatch(outPorts.length - 1));
		requestResponses.put(msg.messageId, new ConcurrentLinkedQueue<Message>());
		for(String port: outPorts){
			Message m1 = new Message(msg);
			m1.receiverId = port;
			Log.i(TAG,"[Add_Msg_Q]: " + m1);
			messageOutQueue.add(m1);
		}
	}

	private int performDelete(String key){
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Delete);
		msg.data = new Row(key);
		Log.i(TAG,"[Delete]: " + msg);
		addMessagesToOutQueue(msg, getNodeHashForKey(key));
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"[Delete]:Exception: ",e);
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


	private long localInsert(String key, String value, String node){
		ContentValues values = new ContentValues();
		values.put(KEY_COLUMN_NAME, key);
		values.put(VALUE_COLUMN_NAME, value);
		values.put(NODE_COLUMN_NAME, node);
		values.put(VERSION_COLUMN_NAME,1);
		long rowId = -1;
		try{
			dbLock.lock();
			Cursor cursor = localQuery(key);
			if(cursor.getCount() > 0 ){
				cursor.moveToFirst();
				int version = cursor.getInt(cursor.getColumnIndex(VERSION_COLUMN_NAME));
				values.put(VERSION_COLUMN_NAME,version+1);
			}
			rowId = db.replace(KEY_VALUE_TABLE, "", values);
			Log.i(TAG,"[Insert]:Local: " +values + " ,rowId: " + rowId);
		}catch (Exception e){
			Log.e(TAG,"[Insert]:Exception: " + values, e);
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
			Log.i(TAG,"[[Query]:Local: " + key + " ,count: " + cursor.getCount());
			cursor.moveToFirst();
		}finally {
			dbLock.unlock();
		}
		return cursor;
	}

	private long performInsert(String key, String value){
		String[] outNodes = getNodeHashForKey(key);
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Insert);
		msg.data =new Row(key, value);
		Log.i(TAG,"[Insert]: " + Arrays.toString(outNodes) + " ,m: " + msg);
		addMessagesToOutQueue(msg, outNodes);
		try{
			operationsTimers.get(msg.messageId).await();
			requestResponses.remove(msg.messageId);
			operationsTimers.remove(msg.messageId);
		} catch (InterruptedException e) {
			Log.e(TAG,"[Insert]:Exception: ",e);
		}
		Log.i(TAG,"[Insert]:Done: " + msg);
		return 1;
	}


	private Cursor performQuery(String key) {
		Log.i(TAG,"[Query]: " + key);
		if(key.equals(LOCAL_NODE)){
			return performLocalGetAll();
		}
		if(key.equals(ENTIRE_RING)){
			return performQueryRing();
		}
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.Query);
		msg.data = new Row(key);
		Log.i(TAG,"[Query]: " + msg +" ,nodes: " + Arrays.toString(getNodeHashForKey(key)));
		addMessagesToOutQueue(msg, getNodeHashForKey(key));
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"[Query]:Exception: ",e);
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
		Log.i(TAG,"[Query]:Done: " + key + ",value: " + value + " ,version: " + version);
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
		Log.i(TAG,"[RowsToCursor]: " + keyValues);
		return cursor;
	}

	private Cursor performLocalGetAll(){
		Cursor cursor;
		try{
			dbLock.lock();
			cursor = db.rawQuery("SELECT key,value from KeyValueTable WHERE value IS NOT NULL", null);
			Log.i(TAG,"[Query]:@:Done: " + cursor.getCount());
		}finally {
			dbLock.unlock();
		}
		if(cursor != null){
			int k = cursor.getColumnIndex(KEY_COLUMN_NAME);
			int v = cursor.getColumnIndex(VALUE_COLUMN_NAME);
			int i = 0;
			for(cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()){
				Log.i(TAG,"[Query]:@:Row:" + (i++) + ":key:"+ cursor.getString(k) + " ,value: " + cursor.getString(v));
			}
			cursor.moveToFirst();
		}
		return cursor;
	}

	private Cursor performQueryRing(){
		Message msg = new Message(messageIDGenerator.incrementAndGet(),myPort,MessageType.AllQuery);
		Log.i(TAG,"[Query]:*: " + msg);
		addMessagesToOutQueue(msg, ports);
		try{
			operationsTimers.get(msg.messageId).await();
		} catch (InterruptedException e) {
			Log.e(TAG,"[Query]:*:Exception: ",e);
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
					Message msgReceived = (Message) input.readObject();
					//Log.i(TAG,"[SERVER]: " + msgReceived);
					msgReceived.requestId = requestIdGenerator.incrementAndGet();
					requestQueue.add(msgReceived);
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
