package com.temp.bitcask.bitcask.core;

public class FileMetaData {
    private String fileId;
    private long startOffset;

    public FileMetaData() {
    }

    public FileMetaData(String fileId, long startOffset) {
        this.fileId = fileId;
        this.startOffset = startOffset;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }
}
