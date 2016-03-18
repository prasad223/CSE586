package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    private HashSet<Integer> REMOTE_PORTS = new HashSet<Integer>(Arrays.asList(11108, 11112, 11116, 11120, 11124));
    final int SERVER_PORT = 10000;
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private static ContentValues contentValues = new ContentValues();
    private HashMap<Integer,HashMap<Integer,Integer>> messageListMap = new HashMap<Integer,HashMap<Integer,Integer>>();
    private HashMap<Integer, Message> messageMap = new HashMap<Integer, Message>();
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
            new FailureDetector().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
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
                synchronized (messageListMap){
                    messageListMap.put(message.messageId, new HashMap<Integer, Integer>());
                }synchronized (messageMap){
                    messageMap.put(message.messageId, message);
                }
                Log.i(TAG, "Generated New Message: " + message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }
        });
    }

    private class FailureDetector extends AsyncTask<Void, Void, Void>{
        @Override
        protected Void doInBackground(Void... params) {
            while(true){
                try {
                    Thread.sleep(2500);
                    publishProgress();
                } catch (InterruptedException e) {
                    Log.e(TAG,"exception: ",e);
                }
            }
        }

        protected void onProgressUpdate(Void... params){
            Message msg = new Message("ping",-1,myPort);
            msg.messageType = MessageType.Ping;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
        }
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
                            proposedSequenceNumber = Math.max(proposedSequenceNumber,currentAgreedSequenceNumber)+1;
                            msgRecieved.sequenceNumber = proposedSequenceNumber;
                            msgRecieved.messageType = MessageType.Proposal;
                            synchronized (priorityBlockingQueue){
                                priorityBlockingQueue.add(msgRecieved);
                            }
                            Log.i(TAG, "NM: pSeqNum: " + proposedSequenceNumber + ", Sending proposal: " + msgRecieved);
                            publishProgress(msgRecieved);
                            break;
                        case Agreement:
                            synchronized (priorityBlockingQueue){
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
                            }

                            break;
                        case Proposal:
                            Log.i(TAG,"PR: " + msgRecieved);
                            synchronized (messageListMap){
                                HashMap<Integer, Integer> proposals = messageListMap.get(msgRecieved.messageId);
                                proposals.put(msgRecieved.proposerId, msgRecieved.sequenceNumber);
                                Log.i(TAG, "PR: " + proposals);
                                if(proposals.size() >= REMOTE_PORTS.size()) {
                                    int proposerId = 0;
                                    int proposalValue = -1;
                                    for(Map.Entry<Integer, Integer> entry: proposals.entrySet()){
                                        if(REMOTE_PORTS.contains(entry.getKey()) && entry.getValue() >= proposalValue){
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

    private class ClientTask extends AsyncTask<Message, Message, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Log.i(TAG,"ClientTASK: msg: " + msgs[0]);
                Message msg = msgs[0];
                switch (msg.messageType){
                    case Agreement:
                    case NewMessage:
                        for(Integer port:REMOTE_PORTS){
                            if(msg.messageType == MessageType.NewMessage){
                                msg.proposerId = port;
                            }
                            sendMessage(msg,port);
                        }
                        break;
                    case Proposal:
                        sendMessage(msg,msg.senderId);
                        break;
                    case Ping:
                        for(int port: REMOTE_PORTS){
                            sendMessage(msg,port);
                        }
                        break;
                    default:
                        Log.i(TAG,"UnhandledMessageType: " + msg);
                        break;
                }
            return null;
        }

        private void sendMessage(Message msg, int port){
            try{
                Log.i(TAG,"SendMessage: port: " + port + " msg: " + msg);
                Socket socket = new  Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
                out.close();
                socket.close();
            }catch (Exception e){
                REMOTE_PORTS.remove(port);
                Log.e(TAG, "This avd is offline: " + port + " new REmote ports: " + REMOTE_PORTS);
                Log.e(TAG, "Exception: ", e);
                handleFailure(port);
            }
        }

        /**
         * 1) Remove all proposals from failed port
         * 2) if they reached agreement then deliver
         * 3) remove messages from queue from the failed sender
         * 4) deliver any other messages from top of the queue if deliverable
         * @param port
         */
        private void handleFailure(int port){
            updateMyMessages(port);
            updateQueue();
            checkMyMessagesForDelivery();
        }

        private void updateMyMessages(int port){
            for(Map.Entry<Integer,HashMap<Integer, Integer>> entry: messageListMap.entrySet()){
                HashMap<Integer,Integer> proposals = entry.getValue();
                if(proposals.containsKey(port)){
                    proposals.remove(port);
                    if(proposals.size() == REMOTE_PORTS.size()) {
                        int proposerId = 0;
                        int proposalValue = -1;
                        for(Map.Entry<Integer, Integer> entry1: proposals.entrySet()){
                            if(entry1.getValue() >= proposalValue){
                                proposalValue = entry1.getValue();
                                proposerId = entry1.getKey();
                            }
                        }
                        Message msg = messageMap.get(entry.getKey());
                        synchronized (messageListMap){
                            messageListMap.remove(msg.messageId);
                        }
                        synchronized (messageMap){
                            messageMap.remove(msg.messageId);
                        }
                        Message msgToSend = new Message(msg.message, msg.senderId, msg.messageId);
                        msgToSend.sequenceNumber = proposalValue;
                        msgToSend.proposerId = proposerId;
                        msgToSend.messageType = MessageType.Agreement;
                        Log.i(TAG, "PR: " + "Agreed Message: " + msgToSend);
                        publishProgress(msgToSend);
                    }
                }
            }
        }

        private void updateQueue(){
            synchronized (priorityBlockingQueue){
                Iterator<Message> queueIterator = priorityBlockingQueue.iterator();
                while(queueIterator.hasNext()){
                    Message msg = queueIterator.next();
                    if(REMOTE_PORTS.contains(msg.senderId)){
                        queueIterator.remove();
                    }
                }
                while(!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().messageType == MessageType.Agreement){
                    Log.i(TAG,"AG: " +"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                    contentValues.put(KEY_FIELD, String.valueOf(messageCounter.getAndIncrement()));
                    contentValues.put(VALUE_FIELD, priorityBlockingQueue.poll().message);
                    Log.i(TAG,"AG: " + "Inserting : " + contentValues);
                    getContentResolver().insert(uri,contentValues);
                }
            }
        }

        private void checkMyMessagesForDelivery(){


        }

        @Override
        protected void onProgressUpdate(Message... values) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values[0]);
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}

