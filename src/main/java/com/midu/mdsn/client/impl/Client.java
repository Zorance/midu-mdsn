package com.midu.mdsn.client.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-15
 */
class Client {
    private TransportCurl http;

    Client(String ip, int port) {
        http = new TransportCurl(ip, port);
    }

    void SetCurlLowSpeedParams(int curlLowSpeedTime, int curlLowSpeedLimit) {
        http.SetCurlLowSpeedParams(curlLowSpeedTime, curlLowSpeedLimit);
    }

    String FileShardPut(ShardTracker shardTracker) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        ArrayList list = Lists.newArrayList();
        map.put("recursive", "false");
        list.add(map);
        Response response = http.Fetch(http.MakeUrl("add", list), shardTracker, shardTracker.shardSize);
        return response.body().string();
    }

    String StoreNodesGet(long bytes, int nodeCount) throws IOException {
        nodeCount = nodeCount <= 0 ? 12 : nodeCount;
        Map<String, String> arg1 = Maps.newHashMap();
        arg1.put("arg", Integer.toString(nodeCount));
        List<Map<String, String>> list = Lists.newArrayList();
        list.add(arg1);

        Map<String, String> arg2 = Maps.newHashMap();

        arg2.put("arg", Long.toString(bytes));
        list.add(arg2);

        String url = http.MakeUrl("storenodes/get", list);
        Response response = http.Fetch(url, null);
        return response.body().string();
    }

    String DataAdd(byte[] data) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        map.put("shardstoreinfo", "true");
        ArrayList list = Lists.newArrayList();
        list.add(map);
        String url = http.MakeUrl("add", list);

        ArrayList<FileUpload> fileUploads = Lists.newArrayList(new FileUpload(Type.kFileContents, "", data));
        Response response = http.Fetch(url, fileUploads);
        return response.body().string();
    }

    byte[] DataGet(String hash) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        map.put("arg", hash);
        ArrayList list = Lists.newArrayList();
        list.add(map);
        String url = http.MakeUrl("object/get", list);
        Response response = http.Fetch(url, null);
        byte[] bytes = response.body().bytes();
        return bytes;
    }

    byte[] FileShardGet(ShardTracker shardTracker) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        map.put("arg", shardTracker.shardHash);
        ArrayList list = Lists.newArrayList();
        list.add(map);
        String url = http.MakeUrl("object/get", list);
        Response response = http.Fetch(url, shardTracker, shardTracker.shardSize);
        byte[] bytes = response.body().bytes();
        return bytes;
    }

    byte[] SummaryGet(String key) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        map.put("arg", key);
        ArrayList list = Lists.newArrayList();
        list.add(map);
        String url = http.MakeUrl("filesummary/get", list);

        Response response = http.Fetch(url, null);
        byte[] bytes = response.body().bytes();
        return bytes;
    }
}