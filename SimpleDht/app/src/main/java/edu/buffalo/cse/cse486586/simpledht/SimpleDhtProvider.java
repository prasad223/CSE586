package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    /*
     *  SQL lite specific declarations
     *  TO create DB and tables
     */
    private SQLiteDatabase db;
    static final String TAG = "SDA";
    private static final String DATABASE_NAME = "DSSpring2016";
    private static final String KEY_VALUE_TABLE = "KeyValueTable";
    private static final String KEY_COLUMN_NAME = "key";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final int DATABASE_VERSION = 1;
    private static final String CREATE_TABLE_SQL_QUERY = "CREATE TABLE " + KEY_VALUE_TABLE +
            "( "+ KEY_COLUMN_NAME +" TEXT PRIMARY KEY NOT NULL, " + VALUE_COLUMN_NAME +" TEXT NOT NULL);";
    static final Uri CONTENT_URI = Uri.parse("edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
    static String myPort;
    static String myHash;
    final int SERVER_PORT = 10000;
    static String LeaderPort = "5554";
    static String ENTIRE_CHORD = "*";
    static String LOCAL_NODE = "@";
    TreeMap<String, String> nodes = new TreeMap<String, String>();
    Lock dbLock = new ReentrantLock(true);

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
        Log.i(TAG,"Port: " + myPort +" , Hash: " + myHash);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket: ", e);
        }

        nodes = new TreeMap<String, String>();
        nodes.put(myHash,myPort);
        if(!myPort.equals(LeaderPort)){
            Message msg = new Message();
            msg.mType = MessageType.Join;
            msg.senderId = myPort;
            msg.receiverId = LeaderPort;
            msg.data = new TreeMap<String, String>();
            msg.data.put(myHash,myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
        return !(db == null);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v("query", selection);
        Message message = new Message();
        message.senderId = myPort;
        message.data = new TreeMap<String, String>();
        message.data.put(MessageType.Query.name(), selection);
        try{
            if(selection.equalsIgnoreCase(ENTIRE_CHORD)){
                message.receiverId = "";
                message.mType = MessageType.AllDelete;
                return (Integer) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message).get();
            }else if(selection.equalsIgnoreCase(LOCAL_NODE)){
                return deleteLocal();
            }else{
                String deleteHash = getHashForKey(selection);
                if(deleteHash.equalsIgnoreCase(myHash)){
                    return db.delete(KEY_VALUE_TABLE, KEY_COLUMN_NAME + "=?", new String[]{selection});
                }else{
                    message.receiverId = nodes.get(deleteHash);
                    message.mType = MessageType.Query;
                    Log.i(TAG,"Deleting: msg: " + message);
                    return (Integer) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message).get();
                }
            }
        }catch (Exception e){
            Log.e(TAG,"Exception: ",e);
        }
        return 0;
    }

    public int deleteLocal(){
        return db.delete(KEY_VALUE_TABLE, null, null);
    }
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        long rowID = -1;
        try{
            dbLock.lock();
            String key = values.getAsString(KEY_COLUMN_NAME);
            String insertHash = getHashForKey(key);
            if(insertHash.equalsIgnoreCase(myHash)){
                rowID = db.replace(KEY_VALUE_TABLE, "", values);
            }else{
                String value = values.getAsString(VALUE_COLUMN_NAME);
                Message msg = new Message();
                msg.senderId = myPort;
                msg.receiverId = nodes.get(insertHash);
                msg.mType = MessageType.Insert;
                msg.data = new TreeMap<String, String>();
                msg.data.put(key, value);
                Log.i(TAG, "Inserting: msg: " + msg);
                Object returnValue = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg).get(1000, TimeUnit.MILLISECONDS);
                if(returnValue != null || !(returnValue instanceof Long)){
                    rowID = (Long) returnValue;
                }
            }
            uri = ContentUris.withAppendedId(CONTENT_URI, rowID);
            Log.i(TAG,"insert: resp: " + uri);
            return uri;
        } catch (Exception e){
            Log.e(TAG,"Insert: Exception ",e);
        }finally {
            dbLock.unlock();
        }
        throw new SQLException("Exception while inserting Key");
    }

    public String getHashForKey(String key){
        String keyHash = genHash(key);
        String highKey = nodes.lastKey();
        String response = "";
        if(keyHash.compareTo(highKey) > 0){
            response = nodes.firstKey();
        }else{
            response= nodes.ceilingKey(keyHash);
        }
        Log.i(TAG,"key: " + key +" ,Hash: " + keyHash + " ,resp: " + response);
        return response;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Log.v("query", selection);
        Message message = new Message();
        message.senderId = myPort;
        message.data = new TreeMap<String, String>();
        try{
            if(selection.equalsIgnoreCase(ENTIRE_CHORD)){
                Log.i(TAG,"Query global");
                message.receiverId = "";
                message.senderId = myPort;
                message.mType = MessageType.AllQuery;
                message.data.put(MessageType.AllQuery.name(), selection);
                Log.i(TAG,"going to query: " + message);
                return (Cursor) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message).get();
            }else if(selection.equalsIgnoreCase(LOCAL_NODE)){
                // Select Local
                Log.i(TAG,"Query Local");
                return queryLocal();
            }else{
                String queryHash = getHashForKey(selection);
                if(queryHash.equalsIgnoreCase(myHash)){
                    return db.query(KEY_VALUE_TABLE, null, KEY_COLUMN_NAME + "=?", new String[]{selection}, null, null, null);
                }else{
                    message.senderId = myPort;
                    message.data.put(MessageType.Query.name(), selection);
                    message.receiverId = nodes.get(queryHash);
                    message.mType = MessageType.Query;
                    Log.i(TAG,"Querying: msg: " + message);
                    return (Cursor) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message).get();
                }
            }
        }catch (Exception e){
            Log.e(TAG,"Exception: ",e);
        }
        return null;
    }

    private Cursor queryLocal(){
        return db.query(KEY_VALUE_TABLE, null, null, null, null, null, null);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input){
        try{
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            //Log.i(TAG,"genHash:Input: "+ input +", output: " + formatter.toString());
            return formatter.toString();
        }catch( NoSuchAlgorithmException e){
            Log.e(TAG,"Exception: ", e);
        }
        return "";
    }

    public TreeMap<String, String> curSorToMap(Cursor cursor){
        TreeMap<String, String> response = new TreeMap<String, String>();
        int kIndex = cursor.getColumnIndex(KEY_COLUMN_NAME);
        int vIndex = cursor.getColumnIndex(VALUE_COLUMN_NAME);
        cursor.moveToFirst();
        for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
            response.put(cursor.getString(kIndex),cursor.getString(vIndex));
        }
        Log.i(TAG," k: " + kIndex +" v: " + vIndex + " cur: " + response);
        return response;
    }

    public Cursor mapToCursor(TreeMap<String, String> values){
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_COLUMN_NAME,VALUE_COLUMN_NAME});
        for(Map.Entry<String, String> entry : values.entrySet()){
            cursor.addRow(new Object[]{entry.getKey(),entry.getValue()});
        }
        Log.i(TAG,"map: " + values+ "\ncur: " + cursor);
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            int i = 0;

            while(true){
                try {
                    Log.i(TAG, "Waiting for new connection " + ++i);
                    Socket socket =  serverSocket.accept();
                    ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream input  = new ObjectInputStream(socket.getInputStream());
                    Message msgRecieved = (Message) input.readObject();
                    Log.i(TAG, "SERVER: msg rec: " + msgRecieved);

                    switch (msgRecieved.mType){
                        case Join:
                            nodes.put(msgRecieved.data.firstKey(), msgRecieved.data.firstEntry().getValue());
                            Message msg = new Message();
                            msg.mType = MessageType.JoinUpdate;
                            msg.senderId = myPort;
                            msg.data = nodes;
                            output.writeObject(null);
                            output.flush();
                            publishProgress(msg);
                            break;
                        case JoinUpdate:
                            nodes = msgRecieved.data;
                            output.writeObject(null);
                            output.flush();
                            Log.i(TAG,"Current FingerTable: " + nodes);
                            break;
                        case Insert:
                            try{
                                dbLock.lock();
                                ContentValues values = new ContentValues();
                                values.put(KEY_COLUMN_NAME,msgRecieved.data.firstEntry().getKey());
                                values.put(VALUE_COLUMN_NAME, msgRecieved.data.firstEntry().getValue());
                                Long rowID = db.replace(KEY_VALUE_TABLE, "", values);
                                output.writeObject(rowID);
                                output.flush();
                            }finally {
                                dbLock.unlock();
                            }
                            break;
                        case Delete:
                            try {
                                dbLock.lock();
                                Integer count = db.delete(KEY_VALUE_TABLE, KEY_COLUMN_NAME + "=?", new String[]{msgRecieved.data.firstEntry().getValue()});
                                output.writeObject(count);
                                output.flush();
                            }finally {
                                dbLock.unlock();
                            }
                            break;
                        case Query:
                            try{
                                dbLock.lock();
                                String searchValue = msgRecieved.data.firstEntry().getValue();
                                Cursor cursor = db.query(KEY_VALUE_TABLE, null, KEY_COLUMN_NAME + "=?", new String[]{searchValue}, null, null, null);
                                msgRecieved.mType = MessageType.QueryResponse;
                                msgRecieved.data = curSorToMap(cursor);
                                output.writeObject(msgRecieved);
                                output.flush();
                            }finally {
                                dbLock.unlock();
                            }
                            break;
                        case AllQuery:
                            try{
                                dbLock.lock();
                                Cursor cursor = db.query(KEY_VALUE_TABLE, null, null, null, null, null, null);
                                msgRecieved.mType = MessageType.QueryResponse;
                                msgRecieved.data = curSorToMap(cursor);
                                output.writeObject(msgRecieved);
                                output.flush();
                            }finally {
                                dbLock.unlock();
                            }
                            break;
                        case AllDelete:
                            try{
                                dbLock.lock();
                                Integer count = db.delete(KEY_VALUE_TABLE, null, null);
                                output.writeObject(count);
                                output.flush();
                            }finally {
                                dbLock.unlock();
                            }
                            break;
                        default:
                            Log.i(TAG, "Unknown messageType: " + msgRecieved);
                            break;
                    }
                    input.close();
                    output.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Exception in ServerSocket: ", e);
                }
                Log.i(TAG, "Connection " + i + " closed");
            }
        }

        protected void onProgressUpdate(Message...msgs) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgs[0]);
        }
    }

    private class ClientTask extends AsyncTask<Message, Void, Object> {

        @Override
        protected Object doInBackground(Message... msgs) {
            Message msg = msgs[0];
            Log.i(TAG,"CT: msg : " + msg);
            switch (msg.mType){
                case Join:
                case Insert:
                case Delete:
                    return sendMessage(msg);
                case Query:
                    Message msgRec = (Message) sendMessage(msg);
                    return mapToCursor(msgRec.data);
                case AllQuery:
                    TreeMap<String, String> values = new TreeMap<String, String>();
                    for(String port: nodes.values()){
                        msg.receiverId = port;
                        if(port.equalsIgnoreCase(myPort)){
                            values.putAll(curSorToMap(queryLocal()));
                        }else{
                            Message msgRec1 = (Message) sendMessage(msg);
                            values.putAll(msgRec1.data);
                        }
                    }
                    return mapToCursor(values);
                case AllDelete:
                    int count = 0;
                    for(String port: nodes.values()){
                        msg.receiverId = port;
                        if(port.equalsIgnoreCase(myPort)){
                            count += deleteLocal();
                        }else{
                            count += (Integer) sendMessage(msg);
                        }
                    }
                    return count;
                case JoinUpdate:
                    for(String port: nodes.values()){
                        msg.receiverId = port;
                        sendMessage(msg);
                    }
                    break;
            }
            return null;
        }

        protected Object sendMessage(Message msg){
            Object response = null;
            try{
                Log.i(TAG,"CT: sending: " + msg);
                int port = Integer.parseInt(msg.receiverId) *2;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                out.writeObject(msg);
                out.flush();
                socket.getReceiveBufferSize();
                Log.i(TAG,"CT: sock: rec buff size: " + socket.getReceiveBufferSize() + " i/p available: " + in.available());
                response = in.readObject();
                Log.i(TAG, "CT: Sending msg: Port: " + port + ", msg: " + msg);
                Log.i(TAG, "CT: Recv: " + response);
                out.close();
                in.close();
                out.close();
                socket.close();
            }catch (Exception e){
                Log.e(TAG,"Exception: ",e);
            }
            Log.i(TAG,"CT: Message sent: conn closed: msg: " + msg);
            return response;
        }
    }
}
