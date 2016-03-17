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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */


public class GroupMessengerActivity extends Activity {

    static final String TAG = "GMA";//GroupMessengerActivity.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int[] REMOTE_PORTS = {11108, 11112, 11116, 11120, 11124};
    static final int SERVER_PORT = 10000;
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private static ContentValues contentValues = new ContentValues();
    private static Map<Integer,HashMap<Integer,Integer>> messageListMap = new HashMap<Integer,HashMap<Integer,Integer>>();
    static int myPort;
    AtomicInteger messageCounter = new AtomicInteger(0);
    int proposedSequenceNumber = -1;
    int currentAgreedSequenceNumber = -1;
    static PriorityBlockingQueue<Message> priorityBlockingQueue = new PriorityBlockingQueue<Message>();
    static AtomicInteger messageIdCounter = new AtomicInteger(0);

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

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr)*2;
        Log.i(TAG, " myport: " + myPort);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket: ", e );
            return;
        }
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new Button.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString();// + "\n";
                editText.setText("");
                Message message = new Message(msg, myPort, messageIdCounter.incrementAndGet());
                messageListMap.put(message.messageId, new HashMap<Integer,Integer>());
                Log.i(TAG, "Generated New Message: " + message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }
        });
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
                    ObjectInputStream input  = new ObjectInputStream(socket.getInputStream());
                    Message msgRecieved = (Message) input.readObject();
                    Log.i(TAG, "SERVER: msg rec: " + msgRecieved);
                    input.close();
                    socket.close();
                    switch (msgRecieved.messageType){
                        case NewMessage:
                            //Log.i(TAG, "NM: " + msgRecieved);
                            //Log.i(TAG, "NM: " +"curr Q: " + priorityBlockingQueue);
                            proposedSequenceNumber = Math.max(proposedSequenceNumber,currentAgreedSequenceNumber)+1;
                            //msgRecieved.proposerId = myPort;
                            msgRecieved.sequenceNumber = proposedSequenceNumber;
                            msgRecieved.messageType = MessageType.Proposal;
                            priorityBlockingQueue.add(msgRecieved);
                            Log.i(TAG, "NM: pSeqNum: " + proposedSequenceNumber + ", Sending proposal: " + msgRecieved);
                            publishProgress(msgRecieved);
                            break;
                        case Agreement:
                            //Log.i(TAG,"AG: " + msgRecieved);
                            //Log.i(TAG, "AG: " + "curr Q: " + priorityBlockingQueue);
                            if(priorityBlockingQueue.contains(msgRecieved)){
                                priorityBlockingQueue.remove(msgRecieved);
                            }
                            priorityBlockingQueue.add(msgRecieved);
                            currentAgreedSequenceNumber = Math.max(currentAgreedSequenceNumber,msgRecieved.sequenceNumber);
                            Log.i(TAG,"AG: curAgreedSeqNum: " + currentAgreedSequenceNumber+ " msgReceived: " + msgRecieved);
                            Log.i(TAG,"AG: " + "curr Q: after new msg: " + priorityBlockingQueue);
                            while(!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().messageType == MessageType.Agreement){
                                    Log.i(TAG,"AG: " +"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                                    contentValues.put(KEY_FIELD, String.valueOf(messageCounter.getAndIncrement()));
                                    contentValues.put(VALUE_FIELD, priorityBlockingQueue.poll().message);
                                    Log.i(TAG,"AG: " + "Inserting : " + contentValues);
                                    getContentResolver().insert(uri,contentValues);
                            }

                            break;
                        case Proposal:
                            Log.i(TAG,"PR: " + msgRecieved);
                            HashMap<Integer, Integer> proposals = messageListMap.get(msgRecieved.messageId);
                            proposals.put(msgRecieved.proposerId, msgRecieved.sequenceNumber);
                            Log.i(TAG, "PR: " + proposals);
                            if(proposals.size() == REMOTE_PORTS.length) {
                                int proposerId = 0;
                                int proposalValue = -1;
                                for(Map.Entry<Integer, Integer> entry: proposals.entrySet()){
                                    if(entry.getValue() >= proposalValue){
                                        proposalValue = entry.getValue();
                                        proposerId = entry.getKey();
                                    }
                                }
                                messageListMap.remove(msgRecieved);
                                Message msgToSend = new Message(msgRecieved.message, msgRecieved.senderId, msgRecieved.messageId);
                                msgToSend.sequenceNumber = proposalValue;
                                msgToSend.proposerId = proposerId;
                                msgToSend.messageType = MessageType.Agreement;
                                Log.i(TAG, "PR: " + "Agreed Message: " + msgToSend);
                                publishProgress(msgToSend);
                            }
                            break;
                        default:
                            Log.i(TAG,"Unknown messageType: "+ msgRecieved);
                            break;
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Exception in ServerSocket: ", e);
                }

                Log.i(TAG, "Connection " + i + " closed");
            }
        }

        protected void onProgressUpdate(Message...msgs) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
        }
    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            try{
                Message msg = msgs[0];
                if(msg.messageType == MessageType.Agreement || msg.messageType == MessageType.NewMessage){
                    for(int port:REMOTE_PORTS){
                        if(msg.messageType == MessageType.NewMessage){
                            msg.proposerId = port;
                        }
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(msg);
                        Log.i(TAG, "Client: " + msg + " onport: " + port);
                        out.flush();
                        out.close();
                        socket.close();
                    }
                }else if(msg.messageType == MessageType.Proposal) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.senderId);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msg);
                    Log.i(TAG, "Client:Proposal: " + msg);
                    out.flush();
                    out.close();
                    socket.close();
                  }
                }catch(SocketTimeoutException e){
                    Log.e(TAG, "Socket Timeout Exception: ", e);
                }catch (IOException e) {
                    Log.e(TAG, "Socket IO Exception: ", e);
                }catch (Exception e){
                    Log.e(TAG, "Gen exception: ", e);
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

