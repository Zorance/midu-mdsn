package com.midu.mdsn.client.impl;

import co.nstant.in.cbor.CborException;
import com.backblaze.erasure.ReedSolomon;
import com.google.common.base.Strings;
import com.google.gson.*;
import com.midu.mdsn.client.IMdsnClient;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-10
 */
public class MdsnClientImpl implements IMdsnClient {

    private static MdsnClientImpl mdsnClient;

    private Config config;

    private Addr addr;

    private final IOException NO_NODE_AVAILABLE_EXCEPTION = new IOException("No bridge or store node is available.");

    private final IOException NO_BRIDGEMODE_IS_DIABLED = new IOException("Error bridgemode is diabled!");

    private final String PARITY_FILE_PATH = "tmp/mdsn/";

    private final long SHARD_MINIMUM_SIZE = 1 << 21;
    // private final long SHARD_MINIMUM_SIZE = 200;

    private final long SHARD_FILE_SIZE_LIMIT = 1 << 29;

    private final long FILE_SIZE_LIMIT = SHARD_FILE_SIZE_LIMIT * 2;

    private final int MAX_CONNECTIONS = 100;

    private final boolean CURLE_WRITE_ERROR = false;

    private final boolean USING_FILE_SUMMARY = false;

    private MdsnClientImpl() {
    }

    static {
        mdsnClient = new MdsnClientImpl();
        mdsnClient.init();
    }

    public static MdsnClientImpl build() {
        return mdsnClient;
    }

    private void init() {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            config = new Gson().fromJson(new FileReader(classLoader.getResource("config.json").getPath()), Config.class);
            Config.Signature sign = config.Signature;
            if (null != sign && sign.Enabled) {
                SignCipher.loadSignCipher(classLoader.getResource(sign.Name).getPath());
            }
            Node[] nodes = config.bridgemode ? config.bridge : config.storenode;
            for (Node n : nodes) {
                n.checkNode();
            }
            addr = new Addr(nodes, config.bridgemode ? "bridge" : "storenode");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    class Config {
        boolean bridgemode;
        Node[] bridge;
        Node[] storenode;
        int lowSpeedTime;
        int lowSpeedLimit;
        Signature Signature;
        boolean fecEnable;

        class Signature {
            String Name;
            boolean Enabled;
        }
    }

    public String uploadFile(String fileName) throws IOException {
        if (Strings.isNullOrEmpty(fileName)) {
            throw new IOException("file name is nil");
        }
        if (!config.bridgemode) {
            throw NO_BRIDGEMODE_IS_DIABLED;
        }

        File file = new File(fileName);
        if (!file.exists()) {
            throw new IOException(String.format("%s not exists", fileName));
        }
        if (file.isDirectory()) {
            throw new IOException(String.format("%s is directory", fileName));
        }
        Node node = addr.getOnline();
        if (null == node) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }

        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
        String nodeStr = client.StoreNodesGet(file.length(), 0);

        List<NodeApiInfo> nodeList = NodeApiInfo.ParseNodeInfo(nodeStr);
        if (null == nodeList || nodeList.isEmpty()) {
            throw new IOException("failed to get peers for storing file");
        }

        int fileShards = 0, parityShards = 0;
        String parityFileName = file.getPath();
        boolean fecEnable = config.fecEnable;
        if (fecEnable) {
            fileShards = (int) Math.ceil((nodeList.size() * 3.0) / 5);
            parityShards = nodeList.size() - fileShards;
            System.out.printf("nodes: %d , fileShards: %d , parityShards: %s , fecEnable: %b \n", nodeList.size(), fileShards, parityShards, fecEnable);
        } else {
            if (file.length() <= SHARD_MINIMUM_SIZE) {
                fileShards = 1;
            } else {
                int s = (int) Math.ceil(file.length() * 1.0 / SHARD_MINIMUM_SIZE);
                fileShards = s <= nodeList.size() ? s : nodeList.size();
            }
            System.out.printf("nodes: %d , fileShards: %d , fecEnable: %b \n", nodeList.size(), fileShards, fecEnable);
        }
        long shardSize = (long) Math.ceil(file.length() * 1.0 / fileShards);
        if (shardSize > SHARD_FILE_SIZE_LIMIT) {
            throw new IOException(String.format("The file size should be less than %d * %d", fileShards, SHARD_FILE_SIZE_LIMIT));
        }
        boolean parityFileFd = false, shardFile = false;

        if (parityShards > 0) {
            parityFileFd = createParityFile(file, fileShards, parityShards, parityFileName);
        } else {
            shardFile = createShardFile(file, fileShards, shardSize);
        }

        int shardsCount = fileShards + parityShards;
        ShardTracker[] shardTrackers = new ShardTracker[shardsCount];
        for (int i = 0; i < shardsCount; i++) {
            shardTrackers[i] = new ShardTracker();
        }

        ExecutorService threadPool = Executors.newCachedThreadPool();

        for (int i = 0; i < fileShards; i++) {
            ShardTracker shard = shardTrackers[i];
            shard.type = Type.kFileName;
            shard.peerIp = nodeList.get(i).ip;
            shard.peerPort = nodeList.get(i).port;
            shard.filePointer = parityFileFd || shardFile ? new File(fileName + "." + i) : file;

            shard.shardIndex = i;
            shard.shardSize = (i == (fileShards - 1)) ? (file.length() - i * shardSize) : shardSize;
            shard.putRemain = shard.shardSize;

            UploadFileShard uploadFileShard = new UploadFileShard(shard);
            Future<ShardTracker> result = threadPool.submit(uploadFileShard);

            try {
                shardTrackers[i] = result.get();
            } catch (Exception e) {
                throw new IOException("upload file shard trackers");
            }
        }

        for (int j = fileShards; parityFileFd && j < shardsCount; j++) {
            ShardTracker shard = shardTrackers[j];
            shard.type = Type.kFileName;
            shard.peerIp = nodeList.get(j).ip;
            shard.peerPort = nodeList.get(j).port;
            shard.filePointer = new File(fileName + "." + j);
            shard.shardIndex = j;
            shard.shardSize = shardSize;
            shard.putRemain = shardSize;

            UploadFileShard uploadFileShard = new UploadFileShard(shard);
            Future<ShardTracker> result = threadPool.submit(uploadFileShard);

            try {
                shardTrackers[j] = result.get();
            } catch (Exception e) {
                throw new IOException("upload parity shard trackers");
            }
        }

        ShardStoreInfo storeInfo = new ShardStoreInfo(file.getName(), file.length(), fecEnable);

        for (int i = 0; i < shardsCount; i++) {
            ShardTracker shardTracker = shardTrackers[i];
            if (Strings.isNullOrEmpty(shardTracker.shardHash)) {
                if (parityFileFd) {
                    cleanParityFile(parityFileName, shardsCount);
                }
                throw new IOException("shardHash is nil");
            }

            StoreShard item = new StoreShard();
            item.ip = shardTracker.peerIp;
            item.port = shardTracker.peerPort;
            item.key = shardTracker.shardHash;
            item.peer_id = nodeList.get(i).id;
            item.size = shardTracker.shardSize;
            if (i < fileShards) {
                storeInfo.addStoreShard(item);
            } else {
                storeInfo.addStoreParityShard(item);
            }
        }
        String id = null;
        try {
            id = uploadStoreInfo(storeInfo);
        } catch (CborException e) {
            e.printStackTrace();
        }
        if (parityFileFd || shardFile) {
            cleanParityFile(parityFileName, shardsCount);
        }
        return id;
    }

    private String uploadStoreInfo(ShardStoreInfo shardStoreInfo) throws IOException, CborException {
        byte[] serialize = shardStoreInfo.serialize();
        Node node = addr.getOnline();
        if (null == node) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }
        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
        String result = client.DataAdd(serialize);
        result = result.substring(result.indexOf("{"), result.indexOf("}") + 1);

        JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
        JsonArray jsonArray = jsonObject.getAsJsonArray("Hash");
        JsonElement element = jsonArray.get(0);
        String hash = element.getAsString();
        return hash;
    }

    class UploadFileShard implements Callable<ShardTracker> {
        ShardTracker shardTracker;

        protected UploadFileShard(ShardTracker shardTracker) {
            this.shardTracker = shardTracker;
        }

        @Override
        public ShardTracker call() {
            Client client = new Client(shardTracker.peerIp, shardTracker.peerPort);
            client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
            String result = "";
            try {
                result = client.FileShardPut(shardTracker);
                result = result.substring(result.indexOf("{"), result.indexOf("}") + 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
            JsonArray jsonArray = jsonObject.getAsJsonArray("Hash");
            JsonElement element = jsonArray.get(0);
            String hash = element.getAsString();
            shardTracker.shardHash = hash;
            return shardTracker;
        }
    }

    public int downloadFileData(String cid, String filePath) throws IOException {
        if (Strings.isNullOrEmpty(cid) || Strings.isNullOrEmpty(filePath)) {
            throw new IOException("cid or file path is nul");
        }
        if (!config.bridgemode) {
            throw NO_BRIDGEMODE_IS_DIABLED;
        }
        File file = new File(filePath + "/" + cid);
        if (file.isDirectory()) {
            throw new IOException(String.format("target file path %s/%s is directory", filePath, cid));
        }
        file.delete();

        ShardStoreInfo si;
        if (USING_FILE_SUMMARY) {
            si = downloadFileSummary(cid);
        } else {
            si = downloadStoreInfo(cid);
        }

        if (si == null) {
            throw new IOException("failed to get file store info");
        }
        int shardCount = si.data.size();
        if (shardCount <= 0) {
            throw new IOException("file shards is null");
        }
        boolean fecEnable = si.fec_enable;
        int parityShards = si.fec_shards;
        int fileShards = shardCount - parityShards;

        System.out.printf("fileShards: %d , parityShards: %d , fecEnable: %b \n", fileShards, parityShards, fecEnable);

        Node node = addr.getOnline();
        if (node == null) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }
        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
        String nodes = client.StoreNodesGet(0, MAX_CONNECTIONS);
        List<NodeApiInfo> nodeList = NodeApiInfo.ParseNodeInfo(nodes);

        if (nodeList == null || nodeList.size() == 0) {
            throw new IOException("failed to get peers for getting file");
        }

        ShardTracker[] shardTrackers = new ShardTracker[shardCount];
        for (int i = 0; i < shardCount; i++) {
            shardTrackers[i] = new ShardTracker();
        }

        ExecutorService threadPool = Executors.newCachedThreadPool();
        int shardOffset = 0;
        for (int i = 0; i < shardCount; i++) {
            StoreShard storeShard = si.getStoreShard(i);
            if (storeShard == null) {
                throw new IOException("store shard is nil");
            }

            ShardTracker tracker = shardTrackers[i];
            tracker.peerIp = storeShard.ip;
            tracker.peerPort = storeShard.port;
            tracker.mode = Mode.mDownload;
            tracker.shardIndex = i;
            tracker.shardSize = storeShard.size;
            tracker.shardOffset = shardOffset;
            tracker.putRemain = 0;
            tracker.shardHash = storeShard.key;
            shardOffset += storeShard.size;
            DownloadFileShard downloadFileShard = new DownloadFileShard(tracker);

            Future<ShardTracker> result = threadPool.submit(downloadFileShard);
            try {
                shardTrackers[i] = result.get();
            } catch (Exception e) {
                throw new IOException("download shard trackers");
            }
        }

        if (fecEnable) {
            byte[][] shards = new byte[shardCount][];
            boolean[] shardPresent = new boolean[shardCount];
            int successBlock = 0, shardSize = 0;

            for (int i = 0; i < shardCount; i++) {
                ShardTracker tracker = shardTrackers[i];
                shardSize = (int) tracker.shardSize;
                shards[i] = new byte[shardSize];
                if (tracker.status) {
                    shardPresent[i] = false;
                    continue;
                }
                shards[i] = new byte[shardSize];
                shards[i] = tracker.output;
                successBlock++;
                shardPresent[i] = true;
            }
            if (successBlock < fileShards) {
                throw new IOException("not enough shards present");
            }
            ReedSolomon reedSolomon = ReedSolomon.create(fileShards, parityShards);
            reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
            byte[] allBytes = new byte[shardSize * fileShards];
            for (int i = 0; i < fileShards; i++) {
                System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
            }
            FileOutputStream outputStream = new FileOutputStream(file);

            byte[] bytes = new byte[(int) si.size];
            System.arraycopy(allBytes, 0, bytes, 0, bytes.length);
            outputStream.write(bytes);
            outputStream.close();
        } else {
            FileOutputStream outputStream = new FileOutputStream(file);
            for (int i = 0; i < fileShards; i++) {
                ShardTracker tracker = shardTrackers[i];
                if (tracker.status) {
                    file.delete();
                    throw new IOException(String.format("failed to download cid %s", cid));
                }
                outputStream.write(tracker.output);
            }
            outputStream.close();
        }
        return 0;
    }

    class DownloadFileShard implements Callable {
        ShardTracker tracker;

        protected DownloadFileShard(ShardTracker shardTracker) {
            tracker = shardTracker;
        }

        @Override
        public ShardTracker call() {
            Client client = new Client(tracker.peerIp, tracker.peerPort);
            client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
            try {
                byte[] bytes = new byte[(int) tracker.shardSize];
                byte[] result = client.FileShardGet(tracker);
                System.arraycopy(result, 0, bytes, 0, result.length);
                tracker.output = bytes;
                tracker.status = false;
            } catch (Exception e) {
                tracker.status = true;
                e.printStackTrace();
            }
            return tracker;
        }
    }

    @Override
    public String uploadFileToNode(String filePath) throws IOException {
        if (config.bridgemode) {
            throw new IOException("Error: bridgemode is enabled!");
        }
        File file = new File(filePath);
        if (file.isDirectory() || !file.exists()) {
            throw new IOException("failed to open the upload file");
        }
        if (file.length() > FILE_SIZE_LIMIT) {
            throw new IOException("The file size should be less than");
        }
        ShardTracker tracker = new ShardTracker();
        tracker.type = Type.kFileName;
        tracker.filePointer = file;
        tracker.shardIndex = 0;
        tracker.shardOffset = 0;
        tracker.shardSize = file.length();
        tracker.totalPut = 0;
        tracker.putRemain = tracker.shardSize;
        tracker.status = true;

        ExecutorService executor = Executors.newSingleThreadExecutor();
        UploadFileShard uploadFileShard = new UploadFileShard(tracker);

        Future<ShardTracker> result = executor.submit(uploadFileShard);

        try {
            tracker = result.get();
        } catch (InterruptedException e) {
            throw new IOException("upload file shard task  interrupted fail");
        } catch (ExecutionException e) {
            throw new IOException("upload file shard task  executionException fail");
        }
        executor.shutdown();

        tracker = uploadFileShard.shardTracker;
        String cid = tracker.shardHash;
        if (Strings.isNullOrEmpty(cid)) {
            throw new IOException(String.format("failed to upload file: %s", filePath));
        }
        System.out.printf("uploaded file: %s cid:", filePath, cid);
        return cid;
    }

    private ShardStoreInfo downloadStoreInfo(String key) throws IOException {
        Node node = addr.getOnline();
        if (node == null) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }
        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
        byte[] bytes = client.DataGet(key);
        ShardStoreInfo shardStoreInfo = ShardStoreInfo.deserialize(bytes);
        List<StoreShard> data = shardStoreInfo.data;
        long max = (long) Math.ceil(data.get(0).size * 1.0);

      /*  long max = 0;
        for (int i = 0; i < data.size(); i++) {
            long size = data.get(i).size;
            max = size > max ? size : max;
        }*/
        for (int i = 0; i < data.size(); i++) {
            data.get(i).size = max;
        }
        return shardStoreInfo;
    }

    private ShardStoreInfo downloadFileSummary(String key) throws IOException {
        Node node = addr.getOnline();
        if (node == null) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }
        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);
        byte[] bytes = client.SummaryGet(key);
        ShardStoreInfo shardStoreInfo = ShardStoreInfo.deserialize(bytes);
        List<StoreShard> data = shardStoreInfo.data;
        long max = 0;
        for (int i = 0; i < data.size(); i++) {
            long size = data.get(i).size;
            max = size > max ? size : max;
        }
        for (int i = 0; i < data.size(); i++) {
            data.get(i).size = max;
        }
        return shardStoreInfo;
    }

    @Override
    public int downloadFileDataFromNode(String cid, String filePath) throws IOException {
        if (config.bridgemode) {
            throw new IOException("Error: bridgemode is enabled!");
        }
        File file = new File(filePath);
        ShardTracker tracker = new ShardTracker();
        tracker.mode = Mode.mDownload;
        tracker.type = Type.kFileName;
        tracker.filePointer = file;
        tracker.shardIndex = 0;
        tracker.shardOffset = 0;
        tracker.shardSize = 0;
        tracker.putRemain = 0;
        tracker.shardHash = cid;

        Node node = addr.getOnline();
        if (node == null) {
            throw NO_NODE_AVAILABLE_EXCEPTION;
        }
        Client client = new Client(node.IP, node.PORT);
        client.SetCurlLowSpeedParams(config.lowSpeedTime, config.lowSpeedLimit);

        byte[] bytes = new byte[(int) tracker.shardSize];
        byte[] result = client.FileShardGet(tracker);
        System.arraycopy(result, 0, bytes, 0, result.length);
        tracker.output = bytes;

        FileOutputStream outputStream = new FileOutputStream(file);
        if (tracker.status) {
            file.delete();
            throw new IOException(String.format("failed to download cid %s", cid));
        }
        outputStream.write(tracker.output);
        outputStream.close();
        return 0;
    }

    private boolean createParityFile(File file, int fileShards, int parityShards, String parityFilename) throws IOException {
        int totalShards = fileShards + parityShards;
        int bytesInInt = 0;
        if (!mapFile(file, true)) {
            throw new NullPointerException();
        }
        int fileSize = (int) file.length();
        final int storedSize = fileSize + bytesInInt;
        final int shardSize = (storedSize + fileShards - 1) / fileShards;

        File tempDir = new File(PARITY_FILE_PATH);
        if (!tempDir.exists()) {
            if (!tempDir.mkdirs()) {
                System.out.printf("mkdir %s fail \n", PARITY_FILE_PATH);
            }
        }
        int bufferSize = shardSize * fileShards;
        byte[] allBytes = new byte[bufferSize];
        InputStream in = new FileInputStream(file);
        int bytesRead = in.read(allBytes, bytesInInt, fileSize);
        if (bytesRead != fileSize) {
            throw new IOException("not enough bytes read");
        }
        in.close();
        byte[][] shards = new byte[totalShards][shardSize];
        for (int i = 0; i < fileShards; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        ReedSolomon reedSolomon = ReedSolomon.create(fileShards, parityShards);
        reedSolomon.encodeParity(shards, 0, shardSize);

        for (int i = 0; i < totalShards; i++) {
            File outputFile = new File(
                    file.getParentFile(),
                    file.getName() + "." + i);
            OutputStream out = new FileOutputStream(outputFile);
            out.write(shards[i]);
            out.close();
        }
        return true;
    }

    private boolean createShardFile(File file, int fileShards, long shardSize) throws IOException {
        int fileSize = (int) file.length();
        byte[] allBytes = new byte[(int) file.length()];
        InputStream in = new FileInputStream(file);
        int bytesRead = in.read(allBytes, 0, fileSize);
        if (bytesRead != fileSize) {
            throw new IOException("not enough bytes read");
        }
        for (int i = 0; i < fileShards; i++) {
            long size = (i == (fileShards - 1)) ? (file.length() - i * shardSize) : shardSize;
            int len = (int) size;
            int offset = i * len;
            File outputFile = new File(
                    file.getParentFile(),
                    file.getName() + "." + i);

            OutputStream out = new FileOutputStream(outputFile);
            byte[] shard = Arrays.copyOfRange(allBytes, offset, len + offset);
            out.write(shard);
            out.close();
        }
        return true;
    }

    private boolean mapFile(File file, boolean readOnly) {
        return readOnly ? file.canRead() : file.canRead() | file.canWrite();
    }

    private void cleanParityFile(String parityFilename, int shardsCount) {
        for (int i = 0; i < shardsCount; i++) {
            String fileName = String.format("%s.%d", parityFilename, i);
            new File(fileName).delete();
        }
    }
}