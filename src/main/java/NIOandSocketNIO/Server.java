package NIOandSocketNIO;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * Created by Administrator on 2018/3/29 0029.
 * source code:https://blog.csdn.net/cauchyweierstrass/article/details/50132733
 */
public class Server {

    private ByteBuffer  buffer = ByteBuffer.allocate(1024*1024);
    Map<SelectionKey,FileChannel> fileMap = new HashMap<SelectionKey,FileChannel>();
    String OutPutPath ="/home/biop/data/cl/MySocketNIO/data/"+ new Date().getTime()+".txt";

    public static void main(String[] args) throws IOException {
        Server server = new Server();
        server.startServer();
    }

    public void startServer() throws IOException {
        Selector selector = Selector.open();
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
//        serverChannel.bind(new InetSocketAddress(8888));
        serverChannel.bind(new InetSocketAddress(38888));
        serverChannel.register(selector,SelectionKey.OP_ACCEPT);
        System.out.println("服务器开启");

        while (true){
            int num = selector.select();
            if(num==0) continue;
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();

            while (it.hasNext()){
                SelectionKey key = it.next();
                if(key.isAcceptable()){
                    ServerSocketChannel serverChannel1 = (ServerSocketChannel) key.channel();
                    SocketChannel socketChannel = serverChannel1.accept();
                    if(socketChannel==null) continue;
                    socketChannel.configureBlocking(false);
                    SelectionKey key1 = socketChannel.register(selector, SelectionKey.OP_READ);
                    InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();

                    //这一句是为了定义，写到哪台远程服务器吗？
//                    File file = new File(remoteAddress.getHostName() + "_" + remoteAddress.getPort() + ".txt");
                    File file = new File(OutPutPath);   //服务器通道文件地址
                    System.out.println(remoteAddress.getHostName()+"_"+remoteAddress.getPort()+".txt");
                    FileChannel fileChannel = new FileOutputStream(file).getChannel();

                    fileMap.put(key1,fileChannel);
                    System.out.println(socketChannel.getRemoteAddress()+"连接成功");
                    writeToClient(socketChannel);
                }else  if(key.isReadable()){
                    readData(key);
                }
                it.remove();
            }
        }

    }

    private void readData(SelectionKey key) throws IOException {
        FileChannel fileChannel = fileMap.get(key);
        buffer.clear();
        SocketChannel socketChannel = (SocketChannel) key.channel();
        int num =0;

        try {
            while ((num = socketChannel.read(buffer))>0){
                buffer.flip();
                fileChannel.write(buffer);
                buffer.clear();
            }
        } catch (IOException e) {
            key.cancel();
            e.printStackTrace();
            return;
        }
        if(num==-1){
            fileChannel.close();
            System.out.println("上传完毕");
            buffer.put((socketChannel.getRemoteAddress()+"上传成功").getBytes());
            buffer.clear();
            socketChannel.write(buffer);
            key.cancel();
        }
    }

    private void writeToClient(SocketChannel socketChannel) throws IOException {
        buffer.clear();
        buffer.put((socketChannel.getRemoteAddress()+"连接成功").getBytes());
        buffer.flip();
        socketChannel.write(buffer);
        buffer.clear();

    }


}



































































