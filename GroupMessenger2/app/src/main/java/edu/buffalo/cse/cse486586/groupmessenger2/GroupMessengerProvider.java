package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

import java.util.HashMap;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 *
 * Please read:
 *
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 *
 * before you start to get yourself familiarized with ContentProvider.
 *
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    /*
     *  SQL lite specific declarations
     *  TO create DB and tables
     */
    private SQLiteDatabase db;
    private static final String DATABASE_NAME = "DSSpring2016";
    private static final String KEY_VALUE_TABLE = "KeyValueTable";
    private static final String KEY_COLUMN_NAME = "key";
    private static final String VALUE_COLUMN_NAME = "value";
    private static final int DATABASE_VERSION = 1;
    private static final String CREATE_TABLE_SQL_QUERY = "CREATE TABLE " + KEY_VALUE_TABLE +
            "( "+ KEY_COLUMN_NAME +" TEXT PRIMARY KEY NOT NULL, " + VALUE_COLUMN_NAME +" TEXT NOT NULL);";

    static final Uri CONTENT_URI = Uri.parse("edu.buffalo.cse.cse486586.groupmessenger2.GroupMessengerProvider");

    private static class DatabaseHelper extends SQLiteOpenHelper{

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
        // If you need to perform any one-time initialization task, please do it here.

        DatabaseHelper databaseHelper = new DatabaseHelper(getContext());
        db = databaseHelper.getWritableDatabase();
        return !(db == null);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        Log.v("insert", values.toString());
        long rowID = db.replace(KEY_VALUE_TABLE, "", values);
        if(rowID > 0)
        {
            uri = ContentUris.withAppendedId(CONTENT_URI, rowID);
            getContext().getContentResolver().notifyChange(uri, null);
            return uri;
        }
        throw new SQLException("Failed to add a record into " + uri);
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
        Log.v("query", selection);

        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(KEY_VALUE_TABLE);
        queryBuilder.setProjectionMap(new HashMap<String, String>());
        queryBuilder.appendWhere(KEY_COLUMN_NAME + " = '" + selection +"'");
        Cursor cursor = queryBuilder.query(db, projection, null, selectionArgs, null, null,sortOrder);
        cursor.setNotificationUri(getContext().getContentResolver(), uri);
        return cursor;
    }
}
