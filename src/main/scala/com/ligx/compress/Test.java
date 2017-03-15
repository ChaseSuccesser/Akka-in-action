package com.ligx.compress;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * Author: ligongxing.
 * Date: 2017年03月06日.
 */
public class Test {

    private static byte[] getSrcBytes() {
        File file = new File("/Users/lgx/work-doc/searchOne2.json");
        try {
            InputStream in = new FileInputStream(file);
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    private static void writeByteToFile(byte[] bytes, String name){
        OutputStream out = null;
        try {
            File file = new File("/Users/lgx/work-doc/" + name +".json");
            out = new FileOutputStream(file);
            IOUtils.write(bytes, out);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void profileTest(boolean isWriteFile) {
        byte[] srcBytes = getSrcBytes();

//        String[] compressCategories = {"bzip2", "deflate", "gzip", "snappy", "lz4", "lzo"};
        String[] compressCategories = {"deflate", "gzip", "snappy"};

        for (String compressName : compressCategories) {
            System.out.println(compressName);
            long startSize = srcBytes.length;
            long startCompressTime = System.currentTimeMillis();
            byte[] compressedBytes = new byte[0];
            switch (compressName) {
                case "gzip":
                    compressedBytes = GZipUtil.compress(srcBytes);
                    break;
                case "snappy":
                    compressedBytes = SnappyUtil.compress(srcBytes);
                    break;
                case "deflate":
                    compressedBytes = DeflateUtil.compress(srcBytes);
                    break;
                case "lz4":
                    compressedBytes = Lz4Util.compress(srcBytes);
                    break;
                case "lzo":
                    compressedBytes = LzoUtil.compress(srcBytes);
                    break;
                case "bzip2":
                    compressedBytes = Bzip2Util.compress(srcBytes);
                    break;
            }
            long endCompressTime = System.currentTimeMillis();
            long endCompressSize = compressedBytes.length;
            System.out.println("压缩前文件大小: " + startSize);
            System.out.println("压缩后文件大小: " + endCompressSize);
            System.out.println("压缩时间: " + (endCompressTime - startCompressTime) + " ms");
            System.out.println("压缩率:" + startSize / endCompressSize + "倍");


            String encodeValue = Base64.encodeBase64String(compressedBytes);
            System.out.println("Base64编码后大小: " + encodeValue.getBytes().length);


            byte[] decompressedBytes = new byte[0];
            long startDecompressTime = System.currentTimeMillis();
            switch (compressName) {
                case "gzip":
                    decompressedBytes = GZipUtil.uncompress(compressedBytes);
                    break;
                case "snappy":
                    decompressedBytes = SnappyUtil.uncompress(compressedBytes);
                    break;
                case "deflate":
                    decompressedBytes = DeflateUtil.uncompress(compressedBytes);
                    break;
                case "lz4":
                    decompressedBytes = Lz4Util.uncompress(compressedBytes);
                    break;
                case "lzo":
                    decompressedBytes = LzoUtil.uncompress(compressedBytes);
                    break;
                case "bzip2":
                    decompressedBytes = Bzip2Util.uncompress(compressedBytes);
                    break;
            }
            long endDecompressTime = System.currentTimeMillis();
            long endDecompressSize = decompressedBytes.length;
            System.out.println("解压后文件大小: " + endDecompressSize);
            System.out.println("解压时间为: " + (endDecompressTime - startDecompressTime) + " ms");

            if(isWriteFile){
                writeByteToFile(decompressedBytes, compressName);
            }

            System.out.println("------------");
        }
    }

    public static void main(String[] args) {
        profileTest(false);
    }
}
