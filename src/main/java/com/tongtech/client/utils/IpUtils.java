package com.tongtech.client.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess.UNSAFE;

/**
 * ip地址与int互转
 */
public class IpUtils {
    public static int IpToInt(String ip) {
        int addr=0;
        try{
            InetAddress address= InetAddress.getByName(ip);
            addr=bytes2int(address.getAddress(), ByteOrder.LITTLE_ENDIAN);
        }catch(UnknownHostException e){
            e.printStackTrace();
        }
        return addr;
    }

    /**
     * 取四个字节的byte数组所代表的int值
     * @param bytes     byte[]
     * @param byteOrder ByteOrder 大小端模式
     * @return int
     */
    private static int bytes2int(byte[] bytes, ByteOrder byteOrder) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(byteOrder);
        return buffer.getInt();
    }

    public static ByteOrder checkByteOrder(){
        long a = UNSAFE.allocateMemory(8);
        ByteOrder byteOrder=null;
        try {
            UNSAFE.putLong(a, 0x0102030405060708L);
            //存放此long类型数据，实际存放占8个字节，01,02,03，04,05,06,07,08
            byte b = UNSAFE.getByte(a);
            //通过getByte方法获取刚才存放的long，取第一个字节
            //如果是大端，long类型顺序存放—》01,02,03,04,05,06,07,08  ，取第一位便是0x01
            //如果是小端，long类型顺序存放—》08,07,06,05,04,03,02,01  ，取第一位便是0x08
            switch (b) {
                case 0x01:
                    byteOrder = ByteOrder.BIG_ENDIAN;
                    break;
                case 0x08:
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                    break;
                default:
                    assert false;
                    byteOrder = null;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            UNSAFE.freeMemory(a);
        }
        return byteOrder;
    }

    /**
     * int转成ip地址
     * @param ipInt
     * @return
     */
    public static String IntToIp(int ipInt) {
        String ip="";
        int[] ipArr = new int[4];
        for (int i = 0; i < 4; i++) {
            ipArr[3 - i] ^= (byte) ipInt & 255;
            ipInt >>>= 8;
        }
        for(int i=0;i<ipArr.length;i++){
            if(i==ipArr.length-1){
                ip=ip+ipArr[i];
            }else {
                ip=ip+ipArr[i]+".";
            }
        }
        return ip;
    }

    public static void main(String[] args) {
        int i=IpToInt("1.10.20.225");
        String ip=IntToIp(i);
        System.out.println( "i="+i+",ip="+ip);
    }

}
