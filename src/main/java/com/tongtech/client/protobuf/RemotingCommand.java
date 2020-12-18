package com.tongtech.client.protobuf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {
    private int verNo;
    private int commandType;
    private int length;
    private static AtomicInteger requestId = new AtomicInteger(1);
    private int code;
    private int opaque = requestId.getAndIncrement();
    private String remark;
    private HashMap<String, String> extFields;
    private transient byte[] body;

    private Object message;

    public Object getMessage() {
        return message;
    }

    public void setMessage(Object message) {
        this.message = message;
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

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public static AtomicInteger getRequestId() {
        return requestId;
    }

    public static void setRequestId(AtomicInteger requestId) {
        RemotingCommand.requestId = requestId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "verNo=" + verNo +
                ", commandType=" + commandType +
                ", length=" + length +
                ", code=" + code +
                ", opaque=" + opaque +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
