package com.midu.mdsn.client.impl;

import com.google.common.base.Strings;
import okhttp3.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-15
 */
class TransportCurl {

    private int curlLowSpeedTime;
    private int curlLowSpeedLimit;

    private BridgeNodeInfo node;

    private String urlPrefix;
    private boolean curlIsSetup = false;
    private OkHttpClient okHttpClient;

    private String URL_API_VERSION = "/api/v0";

    public TransportCurl(String ip, int port) {
        urlPrefix = String.format("http://%s:%d%s", ip, port, URL_API_VERSION);
        node = new BridgeNodeInfo();
        node.reconnect_time = 0;

        okHttpClient = new OkHttpClient().newBuilder().build();
    }

    public void SetCurlLowSpeedParams(int curlLowSpeedTime, int curlLowSpeedLimit) {
        this.curlLowSpeedLimit = curlLowSpeedLimit;
        this.curlLowSpeedTime = curlLowSpeedTime;
    }

    public String MakeUrl(String path, List<Map<String, String>> parameters) {
        String url = String.format("%s/%s", urlPrefix, path);
        int j = 0;
        for (int i = 0; i < parameters.size(); i++) {
            Map<String, String> map = parameters.get(i);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String nameUrlEncoded = entry.getKey();

                String valueUrlEncoded = entry.getValue();

                if (0 == j) {
                    url += "?" + nameUrlEncoded + "=" + valueUrlEncoded;
                    j++;
                } else {
                    url += "&" + nameUrlEncoded + "=" + valueUrlEncoded;
                }
            }
        }
        int offset = url.indexOf("?");
        if (offset != -1) {
            String plain = url.substring(offset + 1);
            String sign = SignCipher.makeSign(plain);
            if (!Strings.isNullOrEmpty(sign)) {
                url += "&sign=" + sign;
            }
        }
        return url;
    }


    public Response Fetch(String url, ShardTracker shardTracker, long size) throws IOException {
        if (shardTracker.putRemain == 0) {
            return FetchShard(url, shardTracker, size);
        }

        String contentType = "application/octet-stream";

        MediaType mediaType = MediaType.parse(contentType);

        byte[] bytes = readShardBody(shardTracker);

        RequestBody body = RequestBody.create(mediaType, bytes);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Accept", "*/*")
                .addHeader("content-type", contentType)
                .addHeader("User-Agent", "java-mdsn-api")
                .build();

        Response response = null;
        try {
            for (int i = 0; i < 2; i++) {
                response = okHttpClient.newCall(request).execute();
                if (HttpURLConnection.HTTP_OK == response.code()) {
                    return response;
                }
            }
            throw new IOException("HTTP request failed with status code " + response.code());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    public Response Fetch(String url, List<FileUpload> fileList) {

        String contentType = "application/octet-stream";
        MediaType mediaType = MediaType.parse("multipart/form-data");

        RequestBody body = null;
        Request request = null;

        if (null == fileList || fileList.size() <= 0) {
            body = new FormBody.Builder()
                    .add("CURLFORM_COPYNAME", "default")
                    .add("CURLFORM_BUFFER", "filename.txt")
                    .add("CURLFORM_BUFFERPTR", "")
                    .add("CURLFORM_BUFFERLENGTH", "0")
                    .add("CURLFORM_CONTENTTYPE", contentType).build();
            request = new Request.Builder().url(url).post(body)
                    .addHeader("Content-Type", contentType)
                    .addHeader("Accept", "*/*")
                    .addHeader("name", "default")
                    .addHeader("filename", "default.txt")
                    .addHeader("User-Agent", "java-mdsn-api").build();
        } else {
            for (int i = 0; null != fileList && i < fileList.size(); i++) {
                FileUpload file = fileList.get(i);
                String name = String.format("file%d", i);
                body = RequestBody.create(mediaType, file.data);
                request = new Request.Builder().url(url).post(body)
                        .addHeader("Content-Type", contentType)
                        .addHeader("Accept", "*/*")
                        .addHeader("name", name)
                        .addHeader("filename", file.path)
                        .addHeader("User-Agent", "java-mdsn-api").build();
            }
        }

        try {
            Response response = okHttpClient.newCall(request).execute();
            if (HttpURLConnection.HTTP_OK == response.code()) {
                return response;
            }
            throw new IOException("HTTP request failed with status code " + response.code());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    byte[] readShardBody(ShardTracker tracker) throws IOException {
        int size = (int) tracker.shardSize;
        byte[] result = new byte[size];
        try (BufferedInputStream stream = new BufferedInputStream(new FileInputStream(tracker.filePointer))) {
            stream.read(result);
        } catch (Exception e) {
            throw e;
        }
        return result;
    }

    Response FetchShard(String url, ShardTracker shardTracker, long size) throws IOException {
        MediaType mediaType = MediaType.parse("multipart/form-data");
        RequestBody body = RequestBody.create(mediaType, new byte[512000]);

        Request request = new Request.Builder().url(url).post(body)
                .addHeader("Accept", "*/*").build();

        Response response = okHttpClient.newCall(request).execute();
        if (HttpURLConnection.HTTP_OK == response.code()) {
            return response;
        }
        throw new IOException("HTTP request failed with status code " + response.code());
    }
}
