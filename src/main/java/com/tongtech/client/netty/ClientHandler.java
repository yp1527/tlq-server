package com.tongtech.client.netty;

import com.tongtech.client.protobuf.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        RemotingCommand remotingCommand=(RemotingCommand)msg;
        CommonMessage message=new CommonMessage();
        message.setCommandType(remotingCommand.getCommandType());
        message.setRequestId(remotingCommand.getOpaque());
        message.setVerNo(0);
        RemotingCommand command=MessageEncoderUtils.MessageEncoderToRemotingCommand(message,remotingCommand);
        ctx.writeAndFlush(command);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
