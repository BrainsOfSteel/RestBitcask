package com.temp.bitcask.bitcask.controller;

import com.temp.bitcask.bitcask.core.Index;
import com.temp.bitcask.bitcask.request.PutKeyRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
public class DatabaseApis {

    @Autowired
    private Index index;

    @PostMapping("/putKey")
    public void putKey(@RequestBody PutKeyRequest request) throws Exception{
        index.put(request.getKey(), request.getValue());
    }

    @GetMapping("/getKey")
    public String getKey(@RequestParam("key")String key) throws IOException {
        return index.get(key);
    }
}
