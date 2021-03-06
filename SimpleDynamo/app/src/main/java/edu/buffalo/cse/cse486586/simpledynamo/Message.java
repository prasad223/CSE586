package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by prasad223 on 4/27/16.
 */

enum MessageType{
    Insert,     InsertResponse,
    Delete,     DeleteResponse,
    Query,      QueryResponse,
    AllDelete,  AllDeleteResponse,
    AllQuery,   AllQueryResponse,
    Sync,       SyncResponse
   }

public class Message implements Serializable {

    final int messageId;
    MessageType mType;
    String senderId;
    String receiverId;
    Object data;
    int requestId;

    Message(int messageId, String senderId, MessageType mType){
        this.messageId = messageId;
        this.senderId = senderId;
        this.mType = mType;
        this.receiverId = null;
        this.data = null;
        this.requestId = Integer.MAX_VALUE;
    }

    public Message(Message message) {
        this.messageId = message.messageId;
        this.senderId = message.senderId;
        this.mType = message.mType;
        this.receiverId = message.receiverId;
        this.data = message.data;
    }

    @Override
    public String toString() {
        return "M{" +
                "Id=" + messageId +
                ", Ty=" + mType +
                ", sId='" + senderId + '\'' +
                ", rId='" + receiverId + '\'' +
                ", rqId='" + requestId + '\'' +
                ", data=" + data +
                '}';
    }
}

class Row implements Serializable{
    int version;
    String key;
    String value;

    Row(String key, String value){
        this.key = key;
        this.value = value;
        this.version = 1;
    }

    Row(String key){
        this(key, null);
    }

    Row() { this(null); }

    @Override
    public String toString() {
        return "Row{" +
                "ver=" + version +
                ", k='" + key + '\'' +
                ", v='" + value + '\'' +
                '}';
    }
}