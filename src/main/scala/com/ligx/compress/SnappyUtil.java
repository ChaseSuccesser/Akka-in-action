package com.ligx.compress;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Author: ligongxing.
 * Date: 2017年03月06日.
 */
public class SnappyUtil {

    public static byte[] compress(byte[] srcBytes){
        try {
            return Snappy.compress(srcBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static byte[] uncompress(byte[] srcBytes){
        try {
            return Snappy.uncompress(srcBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }
}
