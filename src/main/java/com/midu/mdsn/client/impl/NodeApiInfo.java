package com.midu.mdsn.client.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-16
 */
class NodeApiInfo {
    String ip;
    String id;
    int port;

    static List<NodeApiInfo> ParseNodeInfo(String str) {
        JsonObject jsonObject = new JsonParser().parse(str).getAsJsonObject();
        JsonArray jsonArray = jsonObject.getAsJsonArray("nodes");

        Gson gson = new Gson();
        List<NodeApiInfo> nodeList = Lists.newArrayList();

        for (JsonElement user : jsonArray) {
            NodeApiInfo userBean = gson.fromJson(user, new TypeToken<NodeApiInfo>() {
            }.getType());
            nodeList.add(userBean);
        }
        return nodeList;
    }
}

class Node {

    String IP;

    int PORT;

    int status;// 0-down 1-online

    int reconnectTime;

    int icmpTime;

    void checkNode() {
        if (Strings.isNullOrEmpty(IP)) {
            return;
        }
        try {
            Socket socket = new Socket(IP, PORT);
            OutputStream output = socket.getOutputStream();
            PrintWriter writer = new PrintWriter(output, true);
            writer.println("This is a message sent to the server");
            status = 1;
        } catch (Exception e) {
            status = 0;
            e.printStackTrace();
        }
    }
}

class BridgeNodeInfo {
    String mode;
    String ip;
    int port;
    int status;
    int reconnect_time;
    int icmp_time;
}
