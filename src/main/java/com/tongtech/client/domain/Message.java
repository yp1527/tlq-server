package com.tongtech.client.domain;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * 消息体
 */
public class Message implements Serializable {
    private String clientId;
   private String msgId;
   private String header;
   private Byte[] body;
   private Map<String,Object>properties;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public Byte[] getBody() {
        return body;
    }

    public void setBody(Byte[] body) {
        this.body = body;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "Message{" +
                "clientId='" + clientId + '\'' +
                ", msgId='" + msgId + '\'' +
                ", header='" + header + '\'' +
                ", body=" + Arrays.toString(body) +
                ", properties=" + properties +
                '}';
    }
}
