package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.Serializable;

/**
 * Created by prasad223 on 3/16/16.
 */
enum MessageType {NewMessage, Proposal, Agreement, Ping, Ack};
public class Message implements Serializable,Comparable{

    final int messageId;
    MessageType messageType;
    final String message;
    final int senderId;

    int sequenceNumber;
    int proposerId;

    Message(String message,int senderId,int messageId){
        this.message = message;
        this.messageType = MessageType.NewMessage;
        this.senderId = senderId;
        this.messageId = messageId;
        this.sequenceNumber = -1;
        this.proposerId = -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;

        Message message1 = (Message) o;

        if (senderId != message1.senderId) return false;
        if (messageId != message1.messageId) return false;
        return !(message != null ? !message.equals(message1.message) : message1.message != null);

    }

    @Override
    public int hashCode() {
        int result = message != null ? message.hashCode() : 0;
        result = 31 * result + senderId;
        result = 31 * result + messageId;
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "mType=" + messageType +
                ", msg='" + message + '\'' +
                ", sId=" + senderId +
                ", mId=" + messageId +
                ", pId=" + proposerId +
                ", seqNum=" + sequenceNumber +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        if(o == null || !(o instanceof Message)){
            return 1;
        }
        Message other = (Message) o;
        if(this.sequenceNumber == other.sequenceNumber){
            return Integer.compare(this.proposerId,other.proposerId);
        }
        return Integer.compare(this.sequenceNumber,other.sequenceNumber);
    }
}
