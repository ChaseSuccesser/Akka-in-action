package com.ligx.compress;

import net.jpountz.lz4.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Author: ligongxing.
 * Date: 2017年03月06日.
 */
public class Lz4Util {

    public static byte[] compress(byte srcBytes[]) {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, 2048, compressor);
        try {
            compressedOutput.write(srcBytes);
            compressedOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteOutput.toByteArray();
    }

    public static byte[] uncompress(byte[] bytes) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        LZ4FastDecompressor decompresser = LZ4Factory.fastestInstance().fastDecompressor();
        LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(bytes), decompresser);
        try {
            int count;
            byte[] buffer = new byte[2048];
            while ((count = lzis.read(buffer)) != -1) {
                byteOut.write(buffer, 0, count);
            }
            lzis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteOut.toByteArray();
    }
}
