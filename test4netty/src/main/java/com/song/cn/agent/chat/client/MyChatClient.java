package com.song.cn.agent.chat.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MyChatClient {

    public static void main(String[] args) throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        Bootstrap bootStrap = new Bootstrap();
        bootStrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new MyChatClientInitializer());
        final Channel channel = bootStrap.connect("localhost", 8999)
                .sync()
                .channel();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        for (; ; ) {
            String msg = br.readLine();
            if ("stop".equals(msg)) {
                break;
            } else {
                channel.writeAndFlush(msg + "\r\n");
            }
        }
        if (channel != null) {
            channel.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

            }
        });
    }
}
