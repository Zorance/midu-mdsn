package com.midu.mdsn.client;

import java.io.IOException;

/**
 * @author https://xujinbang.com
 * @package com.midu.mdsn.client
 * @date 19-7-10
 */
public interface IMdsnClient {

    /**
     * Upload a file
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    String uploadFile(String fileName) throws IOException;

    /**
     * Download the content with cid
     *
     * @param cid
     * @param filePath
     * @return
     * @throws IOException
     */
    int downloadFileData(String cid, String filePath) throws IOException;

    /**
     * Upload a file to store node
     *
     * @param filePath a file name with full path
     * @return a file content id(cid)
     */
    String uploadFileToNode(String filePath) throws IOException;

    /**
     * Download the content with cid from store node
     *
     * @param cid      the contend id for downloading
     * @param filePath the target path for saving the content file
     * @return 0 success, others failed
     * @throws IOException
     */
    int downloadFileDataFromNode(String cid, String filePath) throws IOException;
}
