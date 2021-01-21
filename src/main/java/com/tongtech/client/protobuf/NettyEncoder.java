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
        //LD: 将消息直接转化为byteBuf流，不需要特殊操作，因为字节屏蔽了所有细节
        out.writeShort(message.getVerNo());
        out.writeShort(message.getCommandType());
        out.writeInt(message.getLength());
        out.writeBytes(message.getBody());
    }
}
