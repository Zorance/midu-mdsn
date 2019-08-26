package com.midu.mdsn.client.test;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.midu.mdsn.client.IMdsnClient;
import com.midu.mdsn.client.impl.MdsnClientImpl;
import com.midu.mdsn.client.impl.SignCipher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.test
 * @date 19-7-10
 */
public class ClientTest {

    // download file id
    String cid = "QmUPYJ4jARbzY7crGLyRYKQvCtwA4GqDWeL1brg7w3mdud";
    // download path
    String filePath = "/home/gavin/test";
    // upload file path
    // String fileName = "/home/gavin/Downloads/ReadMe.MD";
    String fileName = "/home/gavin/test/test.txt";

    static IMdsnClient client;

    @BeforeAll
    public static void TestBuild() {
        client = MdsnClientImpl.build();
    }

    @Test
    public void TestInit() throws Exception {
        if (client == null) {
            throw new IOException("init error");
        }
    }

    @Test
    public void TestUploadFile() throws Exception {
        String id = uploadFile();
        if (Strings.isNullOrEmpty(id)) {
            throw new IOException("upload id is nil");
        }
        System.out.printf("upload file id = %s \n", id);
    }

    String uploadFile() throws Exception {
        return client.uploadFile(fileName);
    }

    @Test
    public void TestDownloadFileData() throws Exception {
        File file = new File(filePath + "/" + cid);
        file.delete();
        int res = client.downloadFileData(cid, filePath);
        if (res != 0) {
            throw new IOException("download result code != 0");
        }
        if (file.exists()) {
            System.out.print("download success");
        } else {
            throw new IOException("download fail");
        }
    }

    @Test
    public void TestUploadAndDownload() throws Exception {
        File uploadFile = new File(fileName);
        // uploadFile.delete();

        FileWriter writer = new FileWriter(fileName);
        List<String> raw = Lists.newArrayList();
        for (int i = 0; i < Byte.MAX_VALUE ; i++) {
            String line = System.currentTimeMillis() + "-" + UUID.randomUUID().toString() + "\n";
            raw.add(line);
            writer.write(line);
        }
        writer.close();
        String id = client.uploadFile(fileName);
        if (Strings.isNullOrEmpty(id)) {
            throw new IOException("upload id is nil");
        }
        System.out.printf("upload file id = %s \n", id);
        int res = client.downloadFileData(id, filePath);
        if (res != 0) {
            throw new IOException("download result code != 0");
        }
        File downloadFile = new File(filePath + "/" + id);
        if (!downloadFile.exists()) {
            throw new IOException("upload error");
        }
        long originalSize = uploadFile.length();
        long downloadSize = downloadFile.length();
        if (originalSize != downloadSize) {
            System.out.printf("original file size:%d , download file size:%d \n", originalSize, downloadSize);
            throw new IOException(" upload and download file size not equal ");
        }
        FileReader reader = new FileReader(filePath + "/" + id);
        BufferedReader bufferedReader = new BufferedReader(reader);
        List<String> result = Lists.newArrayList();
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            result.add(line);
        }
        if (raw.size() != result.size()) {
            throw new IOException(" upload file and download file content not equal ");
        }
        for (int i = 0; i < raw.size(); i++) {
            String l1 = raw.get(i).replaceAll("\n", "");
            String l2 = result.get(i).replaceAll("\n", "");
            if (!l1.equals(l2)) {
                throw new IOException("file content is not equal ");
            }
        }
        System.out.println("file are undamaged ");
    }

    @Test
    public void TestUploadFileToNode() throws Exception {
        String id = client.uploadFileToNode(filePath);
        if (Strings.isNullOrEmpty(id)) {
            throw new IOException("upload id is nil");
        }
        System.out.printf("upload file id = %s \n", id);
    }

    @Test
    public void TestDownloadFileDataFromNode() throws Exception {
        File file = new File(filePath + "/" + cid);
        file.delete();
        int res = client.downloadFileDataFromNode(cid, filePath);
        if (res != 0) {
            throw new IOException("download result code != 0");
        }
        if (file.exists()) {
            System.out.print("download success");
        } else {

            throw new IOException("download fail");
        }
    }

    @Test
    public void TestConcurrentUpload() throws Exception {
        System.out.println("start time" + new Date());
        int count = 100;
        AtomicInteger suc = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        class U implements Runnable {
            @Override
            public void run() {
                try {
                    String id = client.uploadFile(fileName);
                    if (Strings.isNullOrEmpty(id)) {
                        fail.getAndIncrement();
                    } else {
                        suc.getAndIncrement();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail.getAndIncrement();
                }
            }
        }
        U u = new U();
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(u);
            threads[i].start();
        }
        Thread.sleep(1000);
        System.out.println("success count " + suc.get());
        System.out.println("fail count " + fail.get());
        System.out.println("end time" + new Date());
    }

    @Test
    public void TestLoopUploadFile() throws Exception {
        String s = "";
        int count = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            // over 380 timeout occur
            String cid = uploadFile();

            if (Strings.isNullOrEmpty(cid)) {
                throw new IOException("upload cid is nil");
            }
            if (s.equals("")) {
                s = cid;
            }
            if (!s.equals(cid)) {
                throw new IOException("cid not equals");
            }
            count++;
            System.out.printf("upload file cid = %s  count = %d \n", cid, count);
        }
        long end = System.currentTimeMillis();
        System.out.printf(" upload %d file use time %d \n", count, end - start);
    }

    @Test
    public void TestSign() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        SignCipher.loadSignCipher(classLoader.getResource("mdsn.sg").getPath());
        System.out.println(SignCipher.makeSign("arg=100&arg=0"));
    }

}
