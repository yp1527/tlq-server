package com.tongtech.client.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * udp服务端
 */
public class UdpServer {

    private final Bootstrap bootstrap;
    private final NioEventLoopGroup acceptGroup;
    private Channel channel;

    public void start(String host,int port) throws Exception{
        try {
            channel = bootstrap.bind(host, port).sync().channel();
            System.out.println("UdpServer start success"+port);
            channel.closeFuture().await();
        } finally {
            acceptGroup.shutdownGracefully();
        }
    }


    public static UdpServer getInstance(){
        return UdpServerHolder.INSTANCE;
    }

    private static final class UdpServerHolder{
        static final UdpServer INSTANCE = new UdpServer();
    }

    private UdpServer(){
        bootstrap = new Bootstrap();
        acceptGroup = new NioEventLoopGroup();
        bootstrap.group(acceptGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_SNDBUF, 65535)
                .option(ChannelOption.SO_RCVBUF, 65535)
                // 默认netty udp接收DatagramPacket字节数最大是2048，如果数据大，超过这个限制，就会报错，抛出异常
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(65535))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch)
                            throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new UdpHandler());
                    }
                });
    }

    public static void main(String[] args) {
        try {
            UdpServer.getInstance().start("192.168.56.1", 6666);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
