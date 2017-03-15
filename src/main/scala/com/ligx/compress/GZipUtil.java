package com.ligx.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Author: ligongxing.
 * Date: 2017年03月06日.
 */
public class GZipUtil {

    public static byte[] compress(byte[] srcBytes){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(srcBytes);
            gzip.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(gzip != null){
                try {
                    gzip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return out.toByteArray();
    }

    public static byte[] uncompress(byte[] srcBytes){
        ByteArrayInputStream in = new ByteArrayInputStream(srcBytes);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream gzipIn = null;
        try {
            gzipIn = new GZIPInputStream(in);
            byte[] buffer = new byte[1024];
            int n;
            while((n = gzipIn.read(buffer)) > 0){
                out.write(buffer, 0, n);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(gzipIn != null){
                try {
                    gzipIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return out.toByteArray();
    }
}
