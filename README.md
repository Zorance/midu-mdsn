### midu mdsn java sdk

requisite
- jdk >= 1.8
- maven >= 3.6

features:
- upload file
- download file

todo:
- performance test

use example:
```$java
    import com.midu.mdsn.client.*;

    // upload a file
    IMdsnClient client = MdsnClientImpl.build();
    String id = clent.uploadFile("xxx.txt");
    
    // download a file
    int r = client.downloadFileData(id, ".");
```