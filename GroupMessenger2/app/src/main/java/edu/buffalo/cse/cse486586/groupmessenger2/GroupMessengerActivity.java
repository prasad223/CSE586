package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import org.apache.http.impl.io.ContentLengthInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    private ContentResolver contentResolver;
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private static ContentValues contentValues = new ContentValues();

    static int myPort;
    static AtomicInteger messageCounter = new AtomicInteger(0);
    static AtomicInteger sequenceNumber = new AtomicInteger(0);
    static PriorityBlockingQueue<Message> priorityBlockingQueue = new PriorityBlockingQueue<Message>();


    static class Message implements Serializable,Comparable{

        private static final long serialVersionUID = 237569878453498743L;
        String message;
        int senderId;
        boolean isDeliverable;
        double sequenceNumber;
        int[] proposals;

        Message(String message,int senderId){
            this.message = message;
            this.senderId = senderId;
            this.isDeliverable = false;
            this.sequenceNumber = Double.POSITIVE_INFINITY;
            this.proposals = new int[REMOTE_PORTS.length];
            for(int i=0;i<REMOTE_PORTS.length;i++){
                proposals[i] = Integer.MAX_VALUE;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Message)) return false;

            Message message1 = (Message) o;

            if (senderId != message1.senderId) return false;
            return message.equals(message1.message);

        }

        public void computeSequence(){
            int maxIndex =0;
            int maxProposal =0;
            for(int i =0; i<proposals.length;i++){
                if(proposals[i] >= maxProposal){
                    maxProposal = proposals[i];
                    maxIndex = Math.max(i+1,maxIndex);
                }
            }
            sequenceNumber = (double)maxProposal + (maxIndex * 0.1);
            if(senderId == myPort){
                isDeliverable = maxProposal != Integer.MAX_VALUE;
            }
        }

        @Override
        public String toString(){
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(this.senderId + ",");
            stringBuffer.append(this.message + ",");
            stringBuffer.append(this.isDeliverable+",");
            stringBuffer.append(String.valueOf(sequenceNumber)+",");
            stringBuffer.append(Arrays.toString(proposals));
            return stringBuffer.toString();
        }

        @Override
        public int compareTo(Object o) {
            if(o == null || !(o instanceof Message)){
                return -1;
            }
            return Double.compare(this.sequenceNumber,((Message)o).sequenceNumber);
        }
    }

    static class WrapperMsg{
        Message message;
        int port;

        WrapperMsg(Message message,int port){
            this.message = message;
            this.port = port;
        }
    }

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

    private int getIndexForPort(int portNum){ return (portNum - REMOTE_PORTS[0])/4;  }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        contentResolver = getContentResolver();
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr)*2;
        Log.i(TAG, " myport: " + myPort);

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
                for(int port: REMOTE_PORTS){
                    sendMessage(new WrapperMsg(new Message(msg,myPort),port));
                }
            }
        });
    }

    private synchronized void sendMessage(WrapperMsg wmsg){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,wmsg);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true){
                try {
                    Socket socket =  serverSocket.accept();
                    ObjectInputStream input  = new ObjectInputStream(socket.getInputStream());
                    Object o = input.readObject();
                    if(o == null || !(o instanceof Message)){
                        input.close();
                        socket.close();
                        return null;
                    }
                    Message msg = (Message) o;
                    Log.i(TAG, "msfg rec: " + msg);
                    input.close();
                    socket.close();


                    if(msg.isDeliverable){
                        Log.i(TAG, "msg is deliverable ");
                        if(priorityBlockingQueue.contains(msg)){
                            priorityBlockingQueue.remove(msg);
                        }
                        priorityBlockingQueue.add(msg);
                        while(!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().isDeliverable){
                            Log.i(TAG,"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                            contentValues.put(String.valueOf(messageCounter.getAndIncrement()),priorityBlockingQueue.poll().message);
                        }
                        publishProgress(msg.toString());
                        return null;
                    }
                    int portIndex = getIndexForPort(myPort);
                    Log.i(TAG,"portIndex: " + portIndex + " for port: " + myPort);
                    if(msg.proposals[portIndex] == Integer.MAX_VALUE){
                        msg.proposals[portIndex] = sequenceNumber.incrementAndGet();
                    }
                    Log.i(TAG,"update: " + msg);
                    if(msg.senderId == myPort){
                        Log.i(TAG,"my own msg");
                        if(priorityBlockingQueue.contains(msg)){
                            Log.i(TAG,"msg already presnet");
                            ArrayList<Message> temp = new ArrayList<Message>();
                            priorityBlockingQueue.drainTo(temp);
                            Log.i(TAG, "PBQ: " + temp);
                            Message qMsg = temp.remove(temp.indexOf(msg));
                            Log.i(TAG,"qMsg: " + qMsg);
                            for(int i=0;i<msg.proposals.length;i++){
                                msg.proposals[i] = Math.min(msg.proposals[i],qMsg.proposals[i]);
                            }
                            temp.add(msg);
                            priorityBlockingQueue.addAll(temp);
                            Log.i(TAG,"Q_update: " + priorityBlockingQueue);
                            msg.computeSequence();
                            if(msg.isDeliverable){
                                for (int port : REMOTE_PORTS){
                                    sendMessage(new WrapperMsg(msg,port));
                                }
                            } else{
                                sendMessage(new WrapperMsg(msg,msg.senderId));
                            }
                        }else{
                            Log.i(TAG, "new msg");
                            msg.computeSequence();
                            priorityBlockingQueue.add(msg);
                        }
                    }else{
                        Log.i(TAG,"sending proposal");
                        sendMessage(new WrapperMsg(msg, msg.senderId));
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\n");
            return;
        }
    }

    private class ClientTask extends AsyncTask<WrapperMsg, Void, Void> {

        @Override
        protected synchronized Void doInBackground(WrapperMsg... msgs) {
                try {
                    Message msg = msgs[0].message;
                    int port = msgs[0].port;
                    Log.i(TAG, "Message to send: " + msg + " onport: " + port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    if(socket.isConnected()){
                        out.writeObject(msg);
                    }
                    out.flush();
                    out.close();
                    socket.close();
                }catch(SocketTimeoutException e){
                    Log.e(TAG, "Socket Timeout Exception");
                    Log.e(TAG, e.getMessage());
                    e.printStackTrace();
                }catch (IOException e) {
                    Log.e(TAG, "Socket IO Exception");
                    //Log.e(TAG, e.getMessage());
                    e.printStackTrace();
                }catch (Exception e){
                    Log.e(TAG,"Gen exception");
                    Log.e(TAG,e.getMessage());
                    e.printStackTrace();
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

