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
    private ContentResolver contentResolver;
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private static ContentValues contentValues = new ContentValues();
    private static Map<Message,HashSet<Proposal>> messageListMap = new HashMap<Message, HashSet<Proposal>>();
    static int myPort;
    AtomicInteger messageCounter = new AtomicInteger(0);
    int proposedSequenceNumber = -1;
    int currentAgreedSequenceNumber = -1;
    static PriorityBlockingQueue<Message> priorityBlockingQueue = new PriorityBlockingQueue<Message>();
    static AtomicInteger messageIdCounter = new AtomicInteger(0);


    class Proposal implements Comparable{
        int proposerId;
        int proposalValue;

        @Override
        public int compareTo(Object another) {
            if(another == null || !(another instanceof Proposal)){
                return -1;
            }
            Proposal other =(Proposal)another;
            if(this.proposalValue == other.proposalValue){
                return Integer.compare(this.proposerId,other.proposerId);
            }
            return Integer.compare(this.proposalValue, other.proposalValue);
        }

        @Override
        public String toString() {
            return  "pId=" + proposerId +
                    ", pVal=" + proposalValue +
                    '}';
        }

        @Override
        public int hashCode() {
            return proposerId;
        }

        Proposal(int proposerId,int proposalValue){
            this.proposalValue = proposalValue;
            this.proposerId = proposerId;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Proposal)) return false;
            Proposal proposal = (Proposal) o;
            return proposerId == proposal.proposerId;
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
                String msg = editText.getText().toString();// + "\n";
                editText.setText("");
                Message message = new Message(msg, myPort, messageIdCounter.incrementAndGet());
                messageListMap.put(message, new HashSet<Proposal>());
                Log.i(TAG, "Generated New Message: " + message);
                multicastMessage(message);
            }
        });
    }

    private synchronized void multicastMessage(Message msg){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    private synchronized void unicastMessage(Message msg){
        new ProposalSender().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    private class ProposalSender extends AsyncTask<Message, Void, Void>{

        @Override
        protected Void doInBackground(Message... msgs){
            try{
                Message msg = msgs[0];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),msg.senderId);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
                out.close();
                socket.close();
            }catch(Exception e){
            }
            return null;
        }
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
                        continue;
                    }
                    Message msgRecieved = (Message) o;
                    Log.i(TAG, "SERVER: msg rec: " + msgRecieved);
                    input.close();
                    socket.close();
                    switch (msgRecieved.messageType){
                        case NewMessage:
                            Log.i(TAG, "NM: " + msgRecieved);
                            Log.i(TAG, "NM: " +"curr Q: " + priorityBlockingQueue);
                            proposedSequenceNumber = Math.max(proposedSequenceNumber,currentAgreedSequenceNumber)+1;
                            msgRecieved.sequenceNumber = proposedSequenceNumber;
                            if(priorityBlockingQueue.contains(msgRecieved)){
                                priorityBlockingQueue.remove(msgRecieved);
                            }
                            priorityBlockingQueue.add(msgRecieved);
                            msgRecieved.messageType = MessageType.Proposal;
                            Log.i(TAG, "NM: pSeqNum: " + proposedSequenceNumber + ", Sending proposal: " + msgRecieved);
                            unicastMessage(msgRecieved);
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
                            while(!priorityBlockingQueue.isEmpty()){
                                Message queueTop = priorityBlockingQueue.peek();
                                if(queueTop.messageType == MessageType.Agreement && queueTop.proposerId != Integer.MAX_VALUE){
                                    Log.i(TAG,"AG: " +"Deliverable msg on Q: " + priorityBlockingQueue.peek());
                                    contentValues.put(KEY_FIELD, String.valueOf(messageCounter.getAndIncrement()));
                                    contentValues.put(VALUE_FIELD, priorityBlockingQueue.poll().message);
                                    Log.i(TAG,"AG: " + "Inserting : " + contentValues);
                                    contentResolver.insert(uri,contentValues);
                                }
                            }
                            publishProgress(msgRecieved.toString());
                            break;
                        case Proposal:
                            Log.i(TAG,"PR: " + msgRecieved);
                            if(messageListMap.containsKey(msgRecieved)){
                                HashSet<Proposal> proposals = messageListMap.get(msgRecieved);
                                Proposal newProposal = new Proposal(msgRecieved.recieverId,(int)msgRecieved.sequenceNumber);
                                proposals.add(newProposal);
                                messageListMap.put(msgRecieved, proposals);
                                Log.i(TAG, "PR: " + proposals);
                                if(messageListMap.get(msgRecieved).size() != REMOTE_PORTS.length){ continue; }
                                Proposal maxProposal = Collections.max(messageListMap.get(msgRecieved));
                                messageListMap.remove(msgRecieved);
                                Message msgToSend = new Message(msgRecieved.message,msgRecieved.senderId,msgRecieved.messageId);
                                msgToSend.sequenceNumber = maxProposal.proposalValue;
                                msgToSend.proposerId = maxProposal.proposerId;
                                msgToSend.messageType = MessageType.Agreement;
                                Log.i(TAG,"PR: " + "Agreed Message: " + msgToSend);
                                multicastMessage(msgToSend);
                            }
                            break;
                        default:
                            Log.i(TAG,"Unknown messageType: "+ msgRecieved);
                            break;
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

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected synchronized Void doInBackground(Message... msgs) {
            Log.i(TAG,"multicast msg: " + msgs[0] + " REMOTE PORTS:" +Arrays.toString(REMOTE_PORTS));
            for(int port:REMOTE_PORTS){
                try {
                    Message msg = msgs[0];
                    msg.recieverId = port;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msg);
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

