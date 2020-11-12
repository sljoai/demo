package com.song.cn.jvm.bytecode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;

public class MyTest3 {

    public void test() throws IOException{
        try {
            FileInputStream inputStream = new FileInputStream("test.txt");
            ServerSocket serverSocket = new ServerSocket(9099);
            serverSocket.accept();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            System.out.println("finally");
        }
    }
}
