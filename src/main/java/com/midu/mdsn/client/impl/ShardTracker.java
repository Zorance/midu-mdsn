package com.midu.mdsn.client.impl;

import java.io.File;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-11
 */
class ShardTracker {

    Type type;
    Mode mode;
    File filePointer;
    long shardIndex;
    long shardOffset;
    long shardSize;
    long totalPut;
    long putRemain;
    boolean status;
    String shardHash;
    String peerIp;
    int peerPort;
    byte[] output;

}
