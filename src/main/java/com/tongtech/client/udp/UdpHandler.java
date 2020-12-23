package com.tongtech.client.udp;
import com.tongtech.client.netty.CommonMessage;
import com.tongtech.client.netty.MessageDecoderUtils;
import com.tongtech.client.netty.MessageEncoderUtils;
import com.tongtech.client.protobuf.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

/**
 * upd协议处理类
 */
public class UdpHandler extends SimpleChannelInboundHandler<DatagramPacket> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        System.out.println("开始接收来自client的数据");
        ByteBuf in=packet.content();
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
        System.out.println("message:"+message);

        //消息编码
        CommonMessage commonMessage=new CommonMessage();
        commonMessage.setCommandType(commandtype);
        commonMessage.setRequestId(message.getOpaque());
        commonMessage.setVerNo(0);
        RemotingCommand command=MessageEncoderUtils.MessageEncoderToRemotingCommand(commonMessage,message);


        System.out.println("length:"+command.getBody().length);
        ByteBuf out = Unpooled.buffer(8+command.getBody().length);
        out.writeShort(command.getVerNo());
        out.writeShort(command.getCommandType());
        out.writeInt(command.getLength());
        out.writeBytes(command.getBody());

        ctx.writeAndFlush(new DatagramPacket(out,packet.sender()));

    }
}
