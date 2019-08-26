package com.midu.mdsn.client.impl;

import co.nstant.in.cbor.CborBuilder;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborEncoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.builder.ArrayBuilder;
import co.nstant.in.cbor.builder.MapBuilder;
import co.nstant.in.cbor.model.DataItem;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-16
 */
class ShardStoreInfo {

    List<StoreShard> data = Lists.newArrayList();
    String name;
    long size;
    boolean fec_enable;
    int fec_shards;

    private ShardStoreInfo() {
    }

    ShardStoreInfo(String name, long size, boolean fEnable) {
        this.name = name;
        this.size = size;
        this.fec_enable = fEnable;
    }

    void addStoreShard(StoreShard storeShard) {
        data.add(storeShard);
    }

    void addStoreParityShard(StoreShard storeShard) {
        data.add(storeShard);
        fec_shards++;
    }

    byte[] serialize() throws CborException {
        List<List<DataItem>> list = Lists.newArrayList();

        for (StoreShard s : data) {
            MapBuilder<CborBuilder> map = new CborBuilder().startMap();
            map.put("key", s.key);
            map.put("peer_id", s.peer_id);
            map.put("ip", s.ip);
            map.put("port", s.port);
            map.put("size", s.size);
            List<DataItem> item = map.end().build();
            list.add(item);
        }

        MapBuilder<CborBuilder> map = new CborBuilder().addMap();
        map.put("size", size);
        map.put("fec_enable", fec_enable);
        map.put("fec_shards", fec_shards);
        ArrayBuilder<MapBuilder<CborBuilder>> data = map.putArray("data");
        for (List<DataItem> item : list) {
            for (DataItem d : item) {
                data.add(d);
            }
        }
        data.end().end().build();
        List<DataItem> build = map.end().build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new CborEncoder(outputStream).encode(build);
        return outputStream.toByteArray();
    }

    static ShardStoreInfo deserialize(byte[] bytes) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        try {
            List<DataItem> decode = new CborDecoder(byteArrayInputStream).decode();
            for (DataItem d : decode) {
                String s = d.toString();
                // "_" is invalid character
                s = s.replaceAll("\\{_", "{");

                Gson gson = new Gson();
                ShardStoreInfo shardStoreInfo = new ShardStoreInfo();
                shardStoreInfo = gson.fromJson(s, shardStoreInfo.getClass());
                return shardStoreInfo;
            }
        } catch (CborException e) {
            e.printStackTrace();
        }
        return null;
    }

    StoreShard getStoreShard(int index) {
        if (data.size() > index) {
            return data.get(index);
        }
        return null;
    }
}

class StoreShard {
    String key;
    String peer_id;
    String ip;
    int port;
    long size;
}