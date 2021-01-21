package com.tongtech.client.protobuf;

import com.tongtech.client.netty.MessageDecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.charset.Charset;
import java.util.List;

/**
 * 协议格式：version(2)+commandtype(2)+len(4)+消息内容(n)
 * 解码器
 * LD注释：继承ReplayingDecoder的原因是不需要自己
 * 去多次判断可读取的字节位长度，简化操作
 */
public class NettyDecoder extends ReplayingDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) throws Exception {
        //获取指定位置的数值-版本 short 16位
        int version = in.readShort();
        //获取指令类型 16位
        int commandtype=in.readShort();
        //获取长度信息 32位
        int length=in.readInt();
        //获取信息字节数组
        byte[] content = new byte[length];
        in.readBytes(content);
        //消息解码
        RemotingCommand message= MessageDecoderUtils.MessageDecoderToRemotingCommand(content,commandtype,channelHandlerContext);

        message.setLength(length);
        message.setVerNo(version);
        message.setBody(content);
        out.add(message);
    }
}
