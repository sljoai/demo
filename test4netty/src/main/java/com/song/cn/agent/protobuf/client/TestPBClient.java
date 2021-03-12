package com.song.cn.agent.protobuf.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class TestPBClient {

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(bossGroup)
                .handler(new TestPBClientInitializer())
                .channel(NioSocketChannel.class);

        Channel channel = bootstrap.connect("localhost", 8999)
                .sync()
                .channel();
        channel.closeFuture().sync();

        bossGroup.shutdownGracefully();

    }
}
