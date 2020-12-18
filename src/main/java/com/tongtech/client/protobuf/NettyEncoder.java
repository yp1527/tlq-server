package com.tongtech.client.protobuf;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 *  协议格式：version(2)+commandtype(2)+len(4)+消息内容(n)
 * 编码器
 */
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, RemotingCommand message, ByteBuf out) throws Exception {
        out.writeShort(message.getVerNo());
        out.writeShort(message.getCommandType());
        out.writeInt(message.getLength());
        out.writeBytes(message.getBody());
    }
}
