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
 */
public class NettyDecoder extends ReplayingDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) throws Exception {
        int version = in.readShort();
        int commandtype=in.readShort();
        int length=in.readInt();
        byte[] content = new byte[length];
        in.readBytes(content);
        //消息解码
        RemotingCommand message= MessageDecoderUtils.MessageDecoderToRemotingCommand(content,commandtype);
        message.setLength(length);
        message.setVerNo(version);
        message.setBody(content);
        out.add(message);
    }
}
