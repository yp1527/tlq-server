package com.tongtech.client.netty;

import com.google.protobuf.ByteString;
import com.tongtech.client.broker.ProducerManager;
import com.tongtech.client.protobuf.ClientMessageData;
import com.tongtech.client.protobuf.NettyDecoder;
import com.tongtech.client.protobuf.NettyEncoder;
import com.tongtech.client.protobuf.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.*;

import static com.tongtech.client.enums.CB_RESPONSE.CB_RSP_SEND_MESSAGE_ACK;

public class NettyServer {
    //连接map
    public static Map<String, ChannelHandlerContext> map = new HashMap<String, ChannelHandlerContext>();
    public static ProducerManager producerManager = new ProducerManager();
    private final Timer timer = new Timer("ClientHouseKeepingService", true);
    private int port;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() {
        //使用主从模式
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup(4);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            //处理链条定义
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("decoder", new NettyDecoder());
                            pipeline.addLast("encoder", new NettyEncoder());
                            pipeline.addLast(new ClientHandler());
                        }
                    })
                    //TCP的so_backlog参数
                    .option(ChannelOption.SO_BACKLOG, 128)
                    //使用网络层的心跳机制
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            //同步阻塞或者请求结果
            ChannelFuture future = b.bind(port).sync();
            System.out.println("NettyServer start listen at " + port);
            //执行定时任务发送数据
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        //send();
                    } catch (Throwable e) {
                    }
                }
            }, 1000 * 5, 5000);
            future.channel().closeFuture().sync();

        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void send() {
        //测试push模式推送消息
        Iterator<Map.Entry<String, ChannelHandlerContext>> it = map.entrySet().iterator();
//        System.out.println(map);
        while (it.hasNext()) {
            System.out.println("开始执行向客户端发送请求.....");
            Map.Entry<String, ChannelHandlerContext> next = it.next();
            ChannelHandlerContext rep = next.getValue();
            int id = (int) (100 * Math.random() + 100);
            Random r = new Random();
            //int id = r.nextInt(100);ss
            //rep.writeAndFlush(buildMessage(id, "服务端主动请求...."));
            RemotingCommand command = new RemotingCommand();
            ClientMessageData.MessageBuffer.Builder builder =ClientMessageData.MessageBuffer.newBuilder();
            builder.setData(ByteString.copyFromUtf8("server call"));
            //关键参数： Opaque，消息原始对象，指令类型
            command.setOpaque(Integer.parseInt(next.getKey()));
            command.setVerNo(2);
            command.setLength(builder.build().toByteArray().length);
            command.setMessage(builder.build().toByteArray());
            command.setBody(builder.build().toByteArray());
            command.setCommandType(CB_RSP_SEND_MESSAGE_ACK);
            rep.writeAndFlush(command);
            it.remove();
        }
    }

//    public static MessageDataProto.MessageData buildMessage(int requestId, String message) {
//        MessageDataProto.MessageData.Builder builder = MessageDataProto.MessageData.newBuilder();
//        builder.setRequestId(requestId);
//        builder.setStatusCode(300);
//        builder.setData(message);
//        return builder.build();
//    }

    public static void main(String[] args) throws Exception {
        new NettyServer(9999).start();
    }
}
