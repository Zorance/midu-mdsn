package com.midu.mdsn.client.impl;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author https://xujinbang.com/
 * @package com.midu.mdsn.client.impl
 * @date 19-7-11
 */
public class SignCipher {

    private static boolean init;

    private static byte[] key = new byte[32];

    private static byte[] v = new byte[16];

    private static AES aes;

    public static boolean loadSignCipher(String path) throws IOException {
        if (init) {
            return true;
        }
        File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            throw new IOException(String.format("file path %s is no exists or is a directory", path));
        }
        try (BufferedInputStream stream = new BufferedInputStream(new FileInputStream(file))) {
            stream.read(key, 0, 32);
            stream.read(v, 0, 16);
            System.out.printf("Load sign cipher suc. \n");
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        aes = new AES(key, v);
        return init = true;
    }

    public static String makeSign(String plain) {
        if (!init) {
            return null;
        }
        byte[] hax_cons = new byte[8];
        for (int i = 0; i < plain.length(); i++) {
            hax_cons[i % 8] += plain.charAt(i);
        }
        StringBuffer arr_cons = new StringBuffer();
        for (int i = 0; i < 8; i++) {
            arr_cons.append(String.format("%02x", hax_cons[i]));
        }

        String confound = arr_cons.toString();
        byte[] encrypt = aes.encrypt(confound.getBytes());

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 16; i++) {
            buffer.append(String.format("%02x", encrypt[i]));
        }
        return buffer.toString();
    }
}

/**
 * AES-256-CBC
 * encrypt and decrypt
 */
class AES {
    private byte[] k;
    private byte[] v;

    protected AES(byte[] key, byte[] iv) {
        k = key;
        v = iv;
    }

    byte[] encrypt(byte[] context) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");

            SecretKeySpec keySpec = new SecretKeySpec(k, "AES");

            IvParameterSpec ivSpec = new IvParameterSpec(v);

            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);

            byte[] cipherText = cipher.doFinal(context);

            return cipherText;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    String decrypt(byte[] encrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");

            SecretKeySpec keySpec = new SecretKeySpec(k, "AES");

            IvParameterSpec ivSpec = new IvParameterSpec(v);

            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

            byte[] decryptedText = cipher.doFinal(encrypt);

            return new String(decryptedText);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}