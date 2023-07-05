package com.temp.bitcask.bitcask.request;

public class PutKeyRequest {
    private String key;
    private String value;

    public PutKeyRequest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public PutKeyRequest() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
