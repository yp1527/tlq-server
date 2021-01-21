package com.tongtech.client.netty;

import com.tongtech.client.protobuf.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler extends ChannelInboundHandlerAdapter {

    public  int port;
    public  String topic;

    public ClientHandler(int port, String topic) {
        this.port = port;
        this.topic = topic;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //处理读取到的信息 因为已经解码了，所以直接可以转换
        RemotingCommand remotingCommand=(RemotingCommand)msg;
        CommonMessage message=new CommonMessage();
        //将自定义netty指令对象转换为 本地消息对象
        message.setCommandType(remotingCommand.getCommandType());
        message.setRequestId(remotingCommand.getOpaque());
        message.setVerNo(0);
        //LD: 根据来源消息内容发送对于的远程信息回到客户端，应该是一个handle过程
        
        RemotingCommand command=MessageEncoderUtils.MessageEncoderToRemotingCommand(message,remotingCommand,this.port,this.topic);
        ctx.writeAndFlush(command);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
