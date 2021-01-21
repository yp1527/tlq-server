package com.tongtech.client.domain;


import java.io.Serializable;
import java.nio.channels.FileChannel;

public class FileMsg implements Serializable {
    private String fileName;
    private String fileHash;
    private String filePath;

    private long realFileSize;
    private long sourceSize;
    private int index;/* 文件分片编号 */
    private long startPosition;/* 文件分片开始位置 */
    private long endPosition;/* 文件分片结束位置 */
    private int breakFlag; //断点续传标志。服务端默认不删除后台文件。1续传，0不续传
    private String splitFileHash;/* 分片文件hash值 */

    private long fileOffset; //broker上已经存储的文件大小

    private String msgID;
    private long fileID;

    public String getMsgID() {
        return msgID;
    }

    public void setMsgID(String msgID) {
        this.msgID = msgID;
    }

    public long getFileID() {
        return fileID;
    }

    public void setFileID(long fileID) {
        this.fileID = fileID;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }

    private FileChannel fileChannel;

    public long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }

    public long getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(long endPosition) {
        this.endPosition = endPosition;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getSplitFileHash() {
        return splitFileHash;
    }

    public void setSplitFileHash(String splitFileHash) {
        this.splitFileHash = splitFileHash;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileHash() {
        return fileHash;
    }

    public void setFileHash(String fileHash) {
        this.fileHash = fileHash;
    }

    @Override
    public String toString() {
        return "FileMsg{" +
                "fileName='" + fileName + '\'' +
                ", fileHash='" + fileHash + '\'' +
                ", filePath='" + filePath + '\'' +
                ", realFileSize=" + realFileSize +
                ", sourceSize=" + sourceSize +
                ", index=" + index +
                ", startPosition=" + startPosition +
                ", endPosition=" + endPosition +
                ", breakFlag=" + breakFlag +
                ", splitFileHash='" + splitFileHash + '\'' +
                ", fileOffset=" + fileOffset +
                ", msgID='" + msgID + '\'' +
                ", fileID=" + fileID +
                '}';
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getRealFileSize() {
        return realFileSize;
    }

    public void setRealFileSize(long realFileSize) {
        this.realFileSize = realFileSize;
    }

    public long getSourceSize() {
        return sourceSize;
    }

    public void setSourceSize(long sourceSize) {
        this.sourceSize = sourceSize;
    }

    public int getBreakFlag() {
        return breakFlag;
    }

    public void setBreakFlag(int breakFlag) {
        this.breakFlag = breakFlag;
    }
}
