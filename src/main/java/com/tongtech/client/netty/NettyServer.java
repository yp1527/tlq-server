package com.tongtech.client.netty;

import com.tongtech.client.protobuf.NettyDecoder;
import com.tongtech.client.protobuf.NettyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.*;

public class NettyServer {
    //连接map
    public  static Map<String, ChannelHandlerContext> map = new HashMap<String, ChannelHandlerContext>();
    private final Timer timer = new Timer("ClientHouseKeepingService", true);
    private int port;
    public NettyServer(int port){
        this.port = port;
    }
    public void start(){
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup(4);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("decoder",new NettyDecoder());
                            pipeline.addLast("encoder",new NettyEncoder());
                            pipeline.addLast(new ClientHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture future = b.bind(port).sync();
            System.out.println("NettyServer start listen at " + port );
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        send();
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

    public void send(){
        /*Iterator<Map.Entry<String, ChannelHandlerContext>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            System.out.println("开始执行向客户端发送请求.....");
            Map.Entry<String, ChannelHandlerContext> next = it.next();
            ChannelHandlerContext rep = next.getValue();
            int id = (int)(100*Math.random()+100);
            *//*Random r = new Random();
            int id = r.nextInt(100);*//*
            //rep.writeAndFlush(buildMessage(id,"服务端主动请求...."));
        }*/
    }

   /* public static MessageDataProto.MessageData buildMessage(int requestId,String message) {
        MessageDataProto.MessageData.Builder builder = MessageDataProto.MessageData.newBuilder();
        builder.setRequestId(requestId);
        builder.setStatusCode(300);
        builder.setData(message);
        return builder.build();
    }*/

    public static void main(String[] args) throws Exception {
        new NettyServer(9999).start();
    }
}
