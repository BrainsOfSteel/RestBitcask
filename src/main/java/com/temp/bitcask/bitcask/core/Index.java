package com.temp.bitcask.bitcask.core;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

@Component
public class Index {
    private final ConcurrentHashMap<String, FileMetaData> indexData = new ConcurrentHashMap<>();
    private final long MAX_BYTES = 30 * 1024;//30 KBs
    private final String walDirectory = "rollingWalLogs";
    private final RollingLog rollingLog = new RollingLog(walDirectory, MAX_BYTES);

    @PostConstruct
    public void createIndexFromLogs(){
        //create Index from wal files assuming order by timestamp
        File directoryPath = new File(walDirectory);
        File[] fileList = directoryPath.listFiles();
        if(fileList == null || fileList.length == 0){
            return;
        }
        Arrays.sort(fileList, Comparator.comparing(File::getName));
        for(int i =0; i< fileList.length;i++){
            createIndex(fileList[i]);
        }
        System.out.println("Index");
    }

    void createIndex(File file){
        try(BufferedReader br = new BufferedReader(new FileReader(file))){
            String line = null;
            long startOffset=0;
            while((line = br.readLine()) != null){
                String[] content = line.split("\\|");
                long crc = Long.parseLong(content[0]);
                StringBuilder keySize = new StringBuilder();
                int i =0;
                while(i<6){
                    keySize.append(content[1].charAt(i));
                    i++;
                }

                StringBuilder key = new StringBuilder();
                int j=i;
                for(;i<j+Integer.parseInt(keySize.toString(), Character.MAX_RADIX);i++){
                    key.append(content[1].charAt(i));
                }
                long actualCrc = getCrc(content[1]+"\n");
                if(crc != actualCrc){
                    throw new RuntimeException("crc mismatch");
                }
                indexData.put(key.toString(), new FileMetaData(walDirectory +"/"+ file.getName(), startOffset));
                startOffset += line.length()+1;
            }
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("catastrophic failure. Files corrupted");
            System.exit(1);
        }
    }

    public void put(String key, String data) throws IOException {
        FileMetaData fileMetaData = null;
        String logLine = createLogLine(key, data);
        synchronized (rollingLog){
            fileMetaData = rollingLog.log(logLine);
        }
        indexData.put(key, fileMetaData);
    }

    public String get(String key) throws IOException {
        if(indexData.containsKey(key)){
            FileMetaData metaData = indexData.get(key);
            return getDataFromFile(metaData);
        }
        return null;
    }

    private String getDataFromFile(FileMetaData metaData) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(metaData.getFileId()))) {
            reader.skip(metaData.getStartOffset());
            String line = reader.readLine();
            String[] content = line.split("\\|");
            long crcFromFile = Long.parseLong(content[0]);
            long computed = getCrc(content[1]+"\n");
            if(computed != crcFromFile){
                throw new RuntimeException("Corrupted content");
            }
            return getDataFromLine(content[1]+"\n");

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private String getDataFromLine(String s) {
        StringBuilder keySize = new StringBuilder();
        int i =0;
        for(i=0;i<6;i++){
            keySize.append(s.charAt(i));
        }
        i = i+Integer.parseInt(keySize.toString(), Character.MAX_RADIX); //Skip keySize
        i=i+6; // Skip ValueSize
        return s.substring(i, s.length()-1);
    }

    long getCrc(String keyValueContent){
        CRC32 crc32 = new CRC32();
        crc32.update(keyValueContent.getBytes());
        return crc32.getValue();
    }


    private String createLogLine(String key, String data) {
        String keySize = Integer.toString(key.length(), Character.MAX_RADIX);
        keySize = "0".repeat(6-keySize.length()) + keySize;

        data = data+"\n";
        String dataSize = Integer.toString(data.length(), Character.MAX_RADIX);
        dataSize = "0".repeat(6-dataSize.length()) + dataSize;
        String dataLine = keySize+key+dataSize+data;

        return getCrc(dataLine) + "|" + dataLine;
    }

    private static class RollingLog{
        private String fileName;
        private String directoryName = "rollingWalLogs";
        private long MAX_BYTES = 10 * 1024;//10 KBs
        private final String LOG_FILE_PREFIX = "log_";
        private BufferedWriter bufferedFileWriter;
        private File file;

        public RollingLog(String directoryName) {
            this.directoryName = directoryName;
        }

        public RollingLog(String directoryName, long MAX_BYTES) {
            this.directoryName = directoryName;
            this.MAX_BYTES = MAX_BYTES;
        }

        public FileMetaData log(String line) throws IOException {
            try {
                long startOffset = 0;
                if (fileName == null) {
                    fileName = LOG_FILE_PREFIX + System.currentTimeMillis();
                    file = new File(directoryName, fileName);
                    bufferedFileWriter = new BufferedWriter(new FileWriter(file, true));
                }

                if(file.length() + line.length() > MAX_BYTES){
                    bufferedFileWriter.close();
                    fileName = LOG_FILE_PREFIX + System.currentTimeMillis();
                    file = new File(directoryName, fileName);
                    bufferedFileWriter = new BufferedWriter(new FileWriter(file));
                }
                startOffset = file.length();
                bufferedFileWriter.write(line);
                bufferedFileWriter.flush();
                return new FileMetaData(directoryName+ "/"+ fileName, startOffset);
            }catch (Exception e){
                e.printStackTrace();
                bufferedFileWriter.close();
                throw e;
            }
        }
    }
}
