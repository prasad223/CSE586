package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int[] REMOTE_PORTS = {11108, 11112};//, 11116, 11120, 11124};
    static final int SERVER_PORT = 10000;
    static int messageCounter = 0;
    private ContentResolver contentResolver;
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private static ContentValues contentValues = new ContentValues();

    /**
     * buildUri() demonstrates how to build a URI for a ContentProvider.
     *
     * @param scheme
     * @param authority
     * @return the URI
     */
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        contentResolver = getContentResolver();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new Button.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }


    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true){
                try {
                    Socket socket =  serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String[] msgRecieved;
                    String messages;
                    messages = reader.readLine();
                    if(messages != null && messages.trim().length() > 0){
                        msgRecieved= messages.split("\\\n+");
                        if(msgRecieved != null){
                            Log.i(TAG,"recM: " + msgRecieved);
                            contentValues.put(KEY_FIELD,Integer.toString(messageCounter));
                            contentValues.put(VALUE_FIELD, msgRecieved[0].trim());
                            Log.i(TAG,"rec: " + contentValues);
                            contentResolver.insert(uri, contentValues);
                            messageCounter++;
                            publishProgress(msgRecieved);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            for(int port: REMOTE_PORTS){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(),true);
                    String msgToSend = msgs[0];
                    Log.i(TAG, "Message to send: " + msgToSend);
                    if(socket.isConnected()){
                        writer.println(msgToSend);
                    }
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
