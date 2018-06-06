package NIOandSocketNIO;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

/**
 * Created by Administrator on 2018/3/29 0029.
 * source code:https://blog.csdn.net/cauchyweierstrass/article/details/50132733
 */
public class Client {

    public static void main(String[] args) {
        final String path = "E:\\data\\sougouLabs\\SogouQ\\SogouQ\\access_log.20060801.decode.filter";
        final String hostName = "172.16.32.139";
//        final String hostName = "127.0.0.1";
        for(int i=0;i<3;i++){
            new Thread(){
                public void run(){
                    try {
                        SocketChannel socketChannel = SocketChannel.open();
                        socketChannel.socket().connect(new InetSocketAddress(hostName,38888));
                        File file = new File(path);
                        FileChannel fileChannel = new FileInputStream(file).getChannel();
                        ByteBuffer buffer = ByteBuffer.allocate(100);
                        socketChannel.read(buffer);
                        buffer.flip();
                        System.out.println(new String(buffer.array(),0,buffer.limit(), Charset.forName("utf-8")));
                        buffer.clear();
                        int num =0;
                        while ((num=fileChannel.read(buffer))>0){
                            buffer.flip();
                            socketChannel.write(buffer);
                            buffer.clear();
                        }
                        if(num==-1){
                            fileChannel.close();
                            socketChannel.shutdownInput();
                        }
                        socketChannel.read(buffer);
                        buffer.flip();
//                        System.out.println(new String(buffer.array(),0,buffer.limit(),Charset.defaultCharset()));  //源代码：Charset.forName("uft-8")
                        buffer.clear();
                        socketChannel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
        Thread.yield();
    }
}



















































