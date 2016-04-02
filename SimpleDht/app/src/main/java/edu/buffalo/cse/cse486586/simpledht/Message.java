package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * Created by prasad223 on 3/23/16.
 */

enum MessageType{Join, Insert, Query, Delete,
    JoinUpdate, InsertResponse, DeleteResponse, QueryResponse,
    AllQuery, AllDelete}

public class Message implements Serializable {
    String senderId;
    String receiverId;
    TreeMap<String,String> data;
    MessageType mType;

    @Override
    public String toString() {
        return "Msg{" +
                "sId='" + senderId + '\'' +
                ", rId='" + receiverId + '\'' +
                ", mType=" + mType +
                ", data=" + data +
                '}';
    }
}
