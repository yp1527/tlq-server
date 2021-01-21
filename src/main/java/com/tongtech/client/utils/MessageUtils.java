
package com.tongtech.client.utils;

import java.io.*;
import java.util.Map;

/**
 * 请求消息的统一处理工具类
 *
 * @author 杨平
 * @date 2020/6/23
 */
public class MessageUtils {

    /**
     * map转成字节
     * @param map
     * @return
     */
    public static byte[] mapToBytes(Map<String, Object> map) {
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream=null;
        ObjectOutputStream objectOutputStream=null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(map);
            bytes = byteArrayOutputStream.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(byteArrayOutputStream!=null){
                try {
                    byteArrayOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(objectOutputStream!=null){
                try {
                    objectOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return bytes;
    }

    /**
     * 字节转成map
     * @param bytes
     * @return
     */
    public static Object ByteToObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            obj = objectInputStream.readObject();
            byteArrayInputStream.close();
            objectInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return obj;
    }
}
