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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private List<Integer> REMOTE_PORTS = new ArrayList<Integer>(Arrays.asList(11108, 11112, 11116, 11120, 11124));
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
    Lock failureHandleLock = new ReentrantLock();
    boolean startPing = false;
    Thread failureDetector = new FailureDetector();

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
                messageListMap.put(message.messageId, new HashMap<Integer, Integer>());
                messageMap.put(message.messageId, message);
                Log.i(TAG, "Generated New Message: " + message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                if(!startPing){
                    startPing = true;
                    startDetector();
                }
            }
        });
    }

    private void startDetector(){ failureDetector.start(); }

    private class FailureDetector extends Thread{
        @Override
        public void run() {
           // Log.i(TAG,"Started Running Detector");
            while(REMOTE_PORTS.size() != 4){
                try {
                    Thread.sleep(3000);
                    Message msg = new Message("ping",-1,myPort);
                    msg.messageType = MessageType.Ping;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                } catch (InterruptedException e) {
                    Log.e(TAG, "Detector:exception: ", e);
                }
            }
            //Log.i(TAG,"Detector Shutdown");
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            int i = 0;
            while(true){
                try {
                    //Log.i(TAG, "Waiting for new connection " + ++i);
                    Socket socket =  serverSocket.accept();
                    failureHandleLock.lock();
                    Log.i(TAG, "SERVER: Acquired Lock");
                    if(!startPing){
                        Log.i(TAG,"SERVER:Starting failure detector on first message");
                        startPing = true;
                        Message ping = new Message("ping",-1,-1);
                        ping.messageType = MessageType.Ping;
                        publishProgress(ping);
                    }
                    ObjectInputStream input  = new ObjectInputStream(socket.getInputStream());
                    Message msgRecieved = (Message) input.readObject();

                    input.close();
                    socket.close();

                    Log.i(TAG, "SERVER: msg rec: " + msgRecieved);

                    switch (msgRecieved.messageType){
                        case NewMessage:
                            proposedSequenceNumber = Math.max(proposedSequenceNumber,currentAgreedSequenceNumber)+1;
                            msgRecieved.sequenceNumber = proposedSequenceNumber;
                            msgRecieved.messageType = MessageType.Proposal;
                            priorityBlockingQueue.add(msgRecieved);
                            Log.i(TAG, "NM: pSeqNum: " + proposedSequenceNumber + ", Sending proposal: " + msgRecieved);
                            publishProgress(msgRecieved);
                            break;
                        case Agreement:
                            if(priorityBlockingQueue.contains(msgRecieved)){
                                priorityBlockingQueue.remove(msgRecieved);
                            }
                            priorityBlockingQueue.add(msgRecieved);
                            currentAgreedSequenceNumber = Math.max(currentAgreedSequenceNumber,msgRecieved.sequenceNumber);
                            Log.i(TAG,"AG: curAgreedSeqNum: " + currentAgreedSequenceNumber+ " msgReceived: " + msgRecieved);
                            Log.i(TAG,"AG: " + "curr Q: " + priorityBlockingQueue);
                            while(!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().messageType == MessageType.Agreement){
                                Log.i(TAG,"AG: " +"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                                contentValues.put(KEY_FIELD, String.valueOf(messageCounter.getAndIncrement()));
                                contentValues.put(VALUE_FIELD, priorityBlockingQueue.poll().message);
                                Log.i(TAG,"AG: " + "Inserting : " + contentValues);
                                getContentResolver().insert(uri,contentValues);
                            }
                            break;
                        case Proposal:
                            HashMap<Integer, Integer> proposals = messageListMap.get(msgRecieved.messageId);
                            proposals.put(msgRecieved.proposerId, msgRecieved.sequenceNumber);
                            Log.i(TAG,"PR: " + msgRecieved + "PR: " + messageListMap);
                            if(proposals.size() >= REMOTE_PORTS.size()) {
                                int proposerId = 0;
                                int proposalValue = -1;
                                for(Map.Entry<Integer, Integer> entry: proposals.entrySet()){
                                    if(entry.getValue() > proposalValue){
                                        proposalValue = entry.getValue();
                                        proposerId = entry.getKey();
                                    }else if (entry.getValue() == proposalValue){
                                        proposerId = Math.max(proposerId, entry.getKey());
                                    }
                                }
                                messageListMap.remove(msgRecieved.messageId);
                                messageMap.remove(msgRecieved.messageId);
                                Message msgToSend = new Message(msgRecieved.message, msgRecieved.senderId, msgRecieved.messageId);
                                msgToSend.sequenceNumber = proposalValue;
                                msgToSend.proposerId = proposerId;
                                msgToSend.messageType = MessageType.Agreement;
                                Log.i(TAG,"PR:My cur msgs: " + messageListMap + " map: " + messageMap);
                                Log.i(TAG, "PR: " + "Agreed Message: " + msgToSend);
                                publishProgress(msgToSend);
                            }
                            break;
                        default:
                            //Log.i(TAG, "Unknown messageType: "+ msgRecieved);
                            break;
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Exception in ServerSocket: ", e);
                }finally{
                    Log.i(TAG, "SERVER: Released Lock");
                    failureHandleLock.unlock();
                }
                //Log.i(TAG, "Connection " + i + " closed");
            }
        }

        protected void onProgressUpdate(Message...msgs) {
            if(msgs[0].messageType == MessageType.Ping){
                startDetector();
            }else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
            }
        }
    }

    private class ClientTask extends AsyncTask<Message, Message, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            try{
                switch (msg.messageType){
                    case Agreement:
                    case NewMessage:
                    case Ping:
                        Integer[] ports = new Integer[REMOTE_PORTS.size()];
                        REMOTE_PORTS.toArray(ports);
                        for(int port:ports){
                            if(REMOTE_PORTS.contains(port))
                            {
                                sendMessage(msg, port);
                            }
                        }
                        break;
                    case Proposal:
                        sendMessage(msg,msg.senderId);
                        break;
                    default:
                        Log.i(TAG,"UnhandledMessageType: " + msg);
                        break;
                }
            }catch (Exception e){
                Log.e(TAG, "Unknown Exception: ", e);
            }
            return null;
        }

        private void sendMessage(Message msg, int port){
            try{
                if(msg.messageType == MessageType.NewMessage || msg.messageType == MessageType.Ping){
                    msg.proposerId = port;
                }
                Log.i(TAG,"SendMessage: port: " + port + " msg: " + msg);
                Socket socket = new  Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
                out.close();
                socket.close();

            }catch (Exception e){
                Log.e(TAG, "HF: Exception: port: "+port+", msg: " + msg, e);
                Log.e(TAG, "HF: port: " + port + " Remote ports: " + REMOTE_PORTS + ", msg: " + msg);
                handleFailure(port);
                Log.e(TAG, "HF:After: port: " + port + " Remote ports: " + REMOTE_PORTS + ", msg: " + msg);
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
            try{
                failureHandleLock.lock();
                Log.e(TAG, "HF:CLIENT: FAILURE: Acquired Lock : port: " + port);
                Log.e(TAG, "HF:Cur Q: " + priorityBlockingQueue);
                Log.e(TAG, "HF:Cur Msg map: " + messageListMap);
                Log.e(TAG, "HF:if: This avd is offline: " + port + " new REmote ports: " + REMOTE_PORTS);
                if(REMOTE_PORTS.contains(port)){
                    REMOTE_PORTS.remove(new Integer(port));
                    Log.e(TAG, "HF:if: This avd is offline: " + port + " new REmote ports: " + REMOTE_PORTS);
                }
                updateQueue(port);
                updateMyMessages(port);
                checkMyMsgForDelivery();

                Log.i(TAG, "HF:AFTER: Cur Q: " + priorityBlockingQueue);
                Log.i(TAG, "HF:After: Cur Msg map: " + messageListMap);

            }catch (Exception e){
                Log.e(TAG,"Error in handling Failure: port: " + port + " ", e);
            }finally {
                failureHandleLock.unlock();
                Log.e(TAG, "HF: CLIENT: FAILURE: Released Lock");
                Log.e(TAG, "HF:Handling Failure");
                Log.e(TAG, "HF: DOne");
            }
        }

        private void updateMyMessages(int port){
            Log.e(TAG,"HF: Removing entries from Failed port: " + messageListMap);
            for(Map.Entry<Integer,HashMap<Integer, Integer>> entry: messageListMap.entrySet()){
                if(entry.getValue().containsKey(port)){
                    Log.e(TAG,"HF: Removing from following entry: " + entry);
                    entry.getValue().remove(port);
                    Log.e(TAG, "HF: After: Removing from following entry: " + entry);
                }
            }
            Log.e(TAG,"HF: Removing entries Done: " + messageListMap);
        }

        private void checkMyMsgForDelivery(){
            Log.e(TAG,"HF:Checking for any messages Deliverable");
            Iterator<Map.Entry<Integer,HashMap<Integer, Integer>>> mapIterator = messageListMap.entrySet().iterator();
            while (mapIterator.hasNext()){
                Map.Entry<Integer,HashMap<Integer, Integer>> msgEntry = mapIterator.next();
                HashMap<Integer,Integer> proposals = msgEntry.getValue();
                if(proposals.size() >= REMOTE_PORTS.size()) {
                    int proposerId = 0;
                    int proposalValue = -1;
                    for(Map.Entry<Integer, Integer> entry1: proposals.entrySet()){
                        if(entry1.getValue() > proposalValue){
                            proposalValue = entry1.getValue();
                            proposerId = entry1.getKey();
                        }else if(entry1.getValue() == proposalValue){
                            proposerId = Math.max(proposerId,entry1.getKey());
                        }
                    }
                    Message msg = messageMap.get(msgEntry.getKey());
                    mapIterator.remove();
                    messageMap.remove(msg.messageId);
                    Message msgToSend = new Message(msg.message, msg.senderId, msg.messageId);
                    msgToSend.sequenceNumber = proposalValue;
                    msgToSend.proposerId = proposerId;
                    msgToSend.messageType = MessageType.Agreement;
                    Log.e(TAG, "HF: " + "Agreed Message: " + msgToSend);
                    publishProgress(msgToSend);
                }
            }
            Log.e(TAG,"HF:Checking for any messages Deliverable:Done");
        }

        private void updateQueue(int port){
            Log.e(TAG,"HF:UQ:Removing all entries from failed port: Q: " + priorityBlockingQueue);
            Log.e(TAG,"Removing messages from : " + port);
            Iterator<Message> queueIterator = priorityBlockingQueue.iterator();
            while(queueIterator.hasNext()){
                Message msg = queueIterator.next();
                if(!REMOTE_PORTS.contains(msg.senderId)){
                    Log.e(TAG,"Removing msg: " + msg);
                    queueIterator.remove();
                }
            }
            Log.e(TAG,"HF: Queue Updated:Removal: " + priorityBlockingQueue);
            while(!priorityBlockingQueue.isEmpty() && priorityBlockingQueue.peek().messageType == MessageType.Agreement){
                Log.e(TAG,"HF: " +"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                contentValues.put(KEY_FIELD, String.valueOf(messageCounter.getAndIncrement()));
                contentValues.put(VALUE_FIELD, priorityBlockingQueue.poll().message);
                Log.e(TAG,"HF: " + "Inserting : " + contentValues);
                getContentResolver().insert(uri,contentValues);
            }
            Log.e(TAG,"HF: Queue Updated:Done: " + priorityBlockingQueue);
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

