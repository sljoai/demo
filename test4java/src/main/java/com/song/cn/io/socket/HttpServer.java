package com.song.cn.io.socket;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Socket使用案例
 * 测试方法
 * 1. 直接使用浏览器: http://127.0.0.1:8801/
 * 2. 使用工具进行压测
 */
public class HttpServer {
    public static void main(String[] args) {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(8801);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                service(socket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void service(Socket socket) {
        try {
            Thread.sleep(20);
            PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
            printWriter.println("HTTP/1.1 200 OK");
            printWriter.println("Content-Type:text/html;charset=utf-8");
            printWriter.println();
            printWriter.write("hello,nio");
            printWriter.close();
            socket.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
