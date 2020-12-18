package com.tongtech.client.netty;

public class CommonMessage {
    private int verNo;
    private int commandType;
    private String clientId;
    private String groupName;
    private String topic;
    private String consumerId;
    private int pullNum;
    private int recvBufSize;//客户端本地接收缓存剩余大小
    private int ipaddr;
    private int port;
    private int state;
    private int queueID;
    private long maxConsumeQueueOffset;
    private long minConsumeQueueOffset;
    private long consumeHistoryOffset;
    private int ackNum;
    private String producerID;
    //消息内容和长度
    private byte[]body;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getProducerID() {
        return producerID;
    }

    public void setProducerID(String producerID) {
        this.producerID = producerID;
    }

    public long getMinConsumeQueueOffset() {
        return minConsumeQueueOffset;
    }

    public void setMinConsumeQueueOffset(long minConsumeQueueOffset) {
        this.minConsumeQueueOffset = minConsumeQueueOffset;
    }

    public int getQueueID() {
        return queueID;
    }

    public void setQueueID(int queueID) {
        this.queueID = queueID;
    }

    public long getMaxConsumeQueueOffset() {
        return maxConsumeQueueOffset;
    }

    public void setMaxConsumeQueueOffset(long maxConsumeQueueOffset) {
        this.maxConsumeQueueOffset = maxConsumeQueueOffset;
    }

    public long getConsumeHistoryOffset() {
        return consumeHistoryOffset;
    }

    public void setConsumeHistoryOffset(long consumeHistoryOffset) {
        this.consumeHistoryOffset = consumeHistoryOffset;
    }

    public int getAckNum() {
        return ackNum;
    }

    public void setAckNum(int ackNum) {
        this.ackNum = ackNum;
    }

    private Integer requestId;

    public Integer getRequestId() {
        return requestId;
    }

    public void setRequestId(Integer requestId) {
        this.requestId = requestId;
    }

    public int getVerNo() {
        return verNo;
    }

    public void setVerNo(int verNo) {
        this.verNo = verNo;
    }

    public int getCommandType() {
        return commandType;
    }

    public void setCommandType(int commandType) {
        this.commandType = commandType;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public int getPullNum() {
        return pullNum;
    }

    public void setPullNum(int pullNum) {
        this.pullNum = pullNum;
    }

    public int getRecvBufSize() {
        return recvBufSize;
    }

    public void setRecvBufSize(int recvBufSize) {
        this.recvBufSize = recvBufSize;
    }

    public int getIpaddr() {
        return ipaddr;
    }

    public void setIpaddr(int ipaddr) {
        this.ipaddr = ipaddr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
