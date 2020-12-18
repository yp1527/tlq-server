package com.tongtech.client.utils;

import com.sun.management.OperatingSystemMXBean;
import io.netty.util.ReferenceCountUtil;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件工具类
 * @author 杨平
 * @date 2020/10/28
 */
public class FileUtils {
    //读取大小
    public static final int READ_SIZE=1024*1024*4;

    protected static char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9','a', 'b', 'c', 'd', 'e', 'f' };
    protected static MessageDigest messagedigest = null;
    static{
        try{
            messagedigest = MessageDigest.getInstance("MD5");
        }catch(NoSuchAlgorithmException nsaex){
            nsaex.printStackTrace();
        }
    }

    /**
     * 文件流读取
     * @param file
     * @return
     */
    public static String getMD5(File file) {
        String fileMd5=null;
        FileInputStream fileInputStream = null;
        BigInteger bigInteger = null;
        try {
            fileInputStream = new FileInputStream(file);

            byte[] buffer = new byte[READ_SIZE];
            int length = 0;
            while ((length = fileInputStream.read(buffer)) != -1) {
                messagedigest.update(buffer, 0, length);
            }
            byte[] b = messagedigest.digest();
            bigInteger = new BigInteger(1, b);
            fileMd5= bigInteger.toString(16);
        } catch (Exception e) {
        } finally {
            try {
                if (null != fileInputStream) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
            }
        }
        return fileMd5;
    }

    /**
     * FileChannel读取
     * 适用于上G大的文件
     * @param file
     * @return
     * @throws IOException
     */
    public static String getFileMD5(File file) {
        FileInputStream in = null;
        FileChannel ch = null;
        try {
            in = new FileInputStream(file);
            ch = in.getChannel();
            long position=0;
            long offset=READ_SIZE;
            boolean flag=true;
            while (flag){
                if(position+READ_SIZE>=ch.size()){
                    offset=ch.size()-position;
                    flag=false;
                }else {
                    offset=READ_SIZE;
                    flag=true;
                }
                MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, position, offset).load();
                messagedigest.update(byteBuffer);
                position=position+offset;
                byteBuffer.clear();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
            }
        }
        return bufferToHex(messagedigest.digest());
    }

    public static String getFileMD5(File file,long startPosition,long endPosition) {
        FileInputStream in = null;
        FileChannel ch = null;
        try {
            in = new FileInputStream(file);
            ch = in.getChannel();
            long position=startPosition;
            long offset=READ_SIZE;
            boolean flag=true;
            while (flag){
                if(position+READ_SIZE>=endPosition){
                    offset=endPosition-position;
                    flag=false;
                }else {
                    offset=READ_SIZE;
                    flag=true;
                }
                MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, position, offset).load();
                messagedigest.update(byteBuffer);
                position=position+offset;
                byteBuffer.clear();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
            }
        }
        return bufferToHex(messagedigest.digest());
    }

    private static String bufferToHex(byte bytes[]) {
        return bufferToHex(bytes, 0, bytes.length);
    }

    private static String bufferToHex(byte bytes[], int m, int n) {
        StringBuffer stringbuffer = new StringBuffer(2 * n);
        int k = m + n;
        for (int l = m; l < k; l++) {
            appendHexPair(bytes[l], stringbuffer);
        }
        return stringbuffer.toString();
    }


    private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
        char c0 = hexDigits[(bt & 0xf0) >> 4];
        char c1 = hexDigits[bt & 0xf];
        stringbuffer.append(c0);
        stringbuffer.append(c1);
    }

    public static int writeFile(FileChannel fileChannel,byte []data){
        int size=0;
        try {
            if(data!=null && data.length>0){
                ByteBuffer wrap = ByteBuffer.wrap(data);
                size=fileChannel.write(wrap);
                fileChannel.force(true);
                ReferenceCountUtil.release(wrap);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return size;
    }

    /**
     * 创建文件
     * @param path
     * @param fileLength
     * @return
     */
    public static File createFile(String path, int fileLength) {
        File file = new File(path);
        FileChannel channel = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            byte[] b = new byte[1024 * 1024];
            channel = (FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            for(int i = 0; i < fileLength; i++){
                ByteBuffer wrap = ByteBuffer.wrap(b);
                channel.write(wrap);
                channel.force(true);
                ReferenceCountUtil.release(wrap);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    /**
     * 单线程文件切片
     * @param srcPath 源文件地址
     * @param destPath 切分后保存的目录
     * @param count 切分个数
     * @return
     */
    public static List<String> getSplitFile(String srcPath, String destPath, int count){
        List<String>list=new ArrayList<>();
        try {
            File file=new File(srcPath);
            //预分配文件占用磁盘空间“r”表示只读的方式“rw”支持文件随机读取和写入
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            //文件长度
            long length = raf.length();
            //计算切片后，每一文件的大小
            long maxSize = length / count;
            //定义初始文件偏移量（读取文件进度）
            long offset = 0L;
            //开始切割
            for(int i = 0; i < count-1; i++){ //count-1 最后的一份文件不处理
                //初始化
                long fbegin = offset;
                //分割第几份文件
                long fend = (i+1) * maxSize;
                System.out.println("开始："+fbegin+"-----> 结束:"+fend);
                String tmp=destPath+file.getName()+"_"+i+".tmp";
                //把文件块临时文件中
                offset = getChannelWrite(srcPath,tmp,fbegin,fend);
                list.add(tmp);
            }

            //将剩余的写入到最后一份文件中
            if((int)(length - offset) > 0){
                System.out.println("开始："+offset+"-----> 结束:"+length);
                String tmp=destPath+file.getName()+"_"+(count-1)+".tmp";
                getChannelWrite(srcPath,tmp,offset,length);
                list.add(tmp);
            }
        } catch (Exception e) {
        }
        return list;
    }

    public static long getChannelWrite(String srcPath, String destPath, long start, long end){
        RandomAccessFile in=null;
        RandomAccessFile out=null;
        try {
            long startTime=System.currentTimeMillis();
            System.out.println("开始时间:"+startTime);
            //创建只读随机访问文件
            in=new RandomAccessFile(srcPath,"r");
            //创建可读写的随机访问文件
            out=new RandomAccessFile(destPath,"rw");
            //将输入调整到开始位置
            in.seek(start);
            //创建文件输入通道
            FileChannel fileChannelIn=in.getChannel();
            //创建文件输出通道
            FileChannel fileChannelOut=out.getChannel();
            /**
             * fileChannelIn.transferTo最大只能读取2147483647L 其实是字节,也就是2GB - 1个字节的长度,
             * 这就是transferTo单次处理长度的极限长度
             * 所以如果处理超过2GB的文件就要循环执行transferTo方法,直至处理长度和文件长度一样为止
             */
            long size=end-start;
            while (0 < size) {
                long count = fileChannelIn.transferTo(start,size,fileChannelOut);
                System.out.println(Thread.currentThread().getName()+"-->count:"+count);
                if (count > 0) {
                    start += count;
                    size -= count;
                }
            }
            //将字节从此通道的文件传输到给定的可写入字节的输出通道
            //fileChannelIn.transferTo(start,end-start,fileChannelOut);
            long endTime=System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName()+"-传输文件用时:"+(endTime-startTime)+"ms");
        }catch (Exception e){
        }finally {
            try {
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return end;
    }

    public static byte[] getFileData(File file,long position,long size){
        byte[] arr=null;
        FileChannel fileChannel=null;
        try {
            fileChannel = (FileChannel.open(file.toPath(),StandardOpenOption.READ));
            MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size).load();
            map.asReadOnlyBuffer().flip();
            arr = new byte[map.asReadOnlyBuffer().remaining()];
            map.asReadOnlyBuffer().get(arr);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(fileChannel!=null){
                try {
                    fileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return arr;
    }

    /**
     * 获取切分文件的数量
     * @param srcFile 源文件
     * @param fileSize 切分文件大小
     * @return
     */
    public static long getSplitFileNum(File srcFile,long fileSize){
        if(srcFile==null){
            throw new NullPointerException("源文件为空!");
        }
        if(fileSize<=0){
            throw new IllegalArgumentException("分片文件大小必须大于0!");
        }
        if(srcFile.length()<fileSize){
            return 1;
        }
        return srcFile.length()/fileSize;
    }


    public static void main(String[] args) throws IOException {
        String srcPath="D:/netty/send/kk.mp4";
        String destPath="D:/netty/receive/";
       /* long count = getSplitFileNum(new File(srcPath),1024*1024*200);
        System.out.println("文件分片数量:"+count);
        CountDownLatch countDownLatch=new CountDownLatch((int) count);
        List<String> list=getMultiSplitFile(srcPath,destPath,(int) count,countDownLatch);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(list);*/

        System.out.println("开始合并数据。。。。");
        List<String> list=new ArrayList<>();
        list.add("D:/netty/receive/service/tmp_0kk.mp4");
        list.add("D:/netty/receive/service/tmp_1kk.mp4");
        list.add("D:/netty/receive/service/tmp_2kk.mp4");
        list.add("D:/netty/receive/service/tmp_3kk.mp4");
        mergeChannelFile(list,destPath,"999.mp4");
        //7faa8bf787ab28dd1027b62fb3cb6251
        String kk=getFileMD5(new File("D:/netty/receive/999.mp4"));
        System.out.println(kk);

    }

    /**
     * 文件流合并
     * @param srcPathList
     * @param descPath
     * @param fileName
     */
    public static void mergeFile(List<String> srcPathList,String descPath,String fileName){
        InputStream is = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(descPath+fileName, "rw");
            for(String path:srcPathList){
                File file=new File(path);
                is = new FileInputStream(file);
                int len=0;
                byte[] buff = new byte[2048];
                while((len=is.read(buff))!=-1){
                    raf.write(buff,0,len);
                }
                file.delete();
            }
            is.close();
            raf.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * FileChannel合并文件
     * @param srcPathList
     * @param descPath
     * @param fileName
     */
    public static void mergeChannelFile(List<String> srcPathList,String descPath,String fileName){
        RandomAccessFile out=null;
        try {
            long startTime=System.currentTimeMillis();
            System.out.println("开始时间:"+startTime);
            //创建可读写的随机访问文件
            out=new RandomAccessFile(descPath+fileName,"rw");
            //创建文件输出通道
            FileChannel fileChannelOut=out.getChannel();
            for(String srcPath:srcPathList){
                //创建文件输入通道
                FileChannel fileChannelIn=new RandomAccessFile(srcPath,"r").getChannel();
                long start=0;
                long size=fileChannelIn.size();
                while (0 < size) {
                    long count = fileChannelIn.transferTo(start,size,fileChannelOut);
                    if (count > 0) {
                        start += count;
                        size -= count;
                    }
                }
                fileChannelIn.close();
            }
            long endTime=System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName()+"-合并文件用时:"+(endTime-startTime)+"ms");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if(out!=null){
                    out.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    public static void getDiskInfo() {
        File[] disks = File.listRoots();
        for(File file : disks) {
            System.out.print(file.getPath() + "    ");
            System.out.print("空闲未使用 = " + file.getFreeSpace() / 1024 / 1024 + "M" + "    ");// 空闲空间
            System.out.print("已经使用 = " + file.getUsableSpace() / 1024 / 1024 + "M" + "    ");// 可用空间
            System.out.print("总容量 = " + file.getTotalSpace() / 1024 / 1024 + "M" + "    ");// 总空间
            System.out.println();
        }
    }

    public static void getMemInfo() {
        OperatingSystemMXBean mem = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        System.out.println("总物理内存大小：" + mem.getTotalPhysicalMemorySize() / 1024 / 1024 + "MB");
        System.out.println("可使用的物理内存大小：" + mem.getFreePhysicalMemorySize() / 1024 / 1024 + "MB");
    }
}
