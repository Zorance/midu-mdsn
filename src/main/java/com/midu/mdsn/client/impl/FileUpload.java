package com.midu.mdsn.client.impl;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-15
 */
class FileUpload {
    Type type;
    String path;
    byte[] data;

    FileUpload(Type t, String p, byte[] d) {
        type = t;
        path = p;
        data = d;
    }
}
