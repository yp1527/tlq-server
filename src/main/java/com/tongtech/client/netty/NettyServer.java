package com.tongtech.client.netty;

import com.tongtech.client.broker.ProducerManager;
import com.tongtech.client.protobuf.NettyDecoder;
import com.tongtech.client.protobuf.NettyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

public class NettyServer {
    //连接map
    public static Map<String, ChannelHandlerContext> map = new HashMap<String, ChannelHandlerContext>();
    public static ProducerManager producerManager = new ProducerManager();
    private final Timer timer = new Timer("ClientHouseKeepingService", true);
    public  int port;
    public  String topic;

    public NettyServer(int port,String topic) {
        this.port = port;
        this.topic=topic;
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
                            pipeline.addLast(new ClientHandler(port,topic));
                        }
                    })
                    //TCP的so_backlog参数
                    .option(ChannelOption.SO_BACKLOG, 128)
                    //使用网络层的心跳机制
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            //同步阻塞或者请求结果
            ChannelFuture future = b.bind(port).sync();
            System.out.println("NettyServer start listen at " + port);
            future.channel().closeFuture().sync();

        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }


    public static void main(String[] args) throws Exception {
        new NettyServer(9999,"topic").start();
    }
}
