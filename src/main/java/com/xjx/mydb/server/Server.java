package com.xjx.mydb.server;

import com.xjx.mydb.server.tbm.TableManager;
import com.xjx.mydb.transport.Encoder;
import com.xjx.mydb.transport.Package;
import com.xjx.mydb.transport.Packager;
import com.xjx.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Xjx
 * @Create: 2023/2/15 - 16:56
 */
public class Server {
    private int port;
    TableManager tbm;

    //MyDB支持多个客户端连接，因此需要区分端口号
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            //Server 启动一个 ServerSocket 监听端口，当有请求到来时直接把请求丢给一个新线程处理
            //每一个请求都相当于一个客户端连接
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        //创建一个线程池用来管理线程，因为每一个请求都由一个线程进行处理
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20,
                1l, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
        //server端开启之后就一直接收来自监听端口的请求然后交给一个HandleSocket执行
        try {
            while (true) {
                Socket socket = ss.accept();
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ioException) {}
        }
    }

    //HandleSocket类实现了 Runnable 接口，在建立连接后初始化 Packager,随后就循环接收来自客户端的数据并处理
    class HandleSocket implements Runnable {
        private Socket socket;
        private TableManager tbm;

        public HandleSocket(Socket socket, TableManager tbm) {
            this.socket = socket;
            this.tbm = tbm;
        }

        //请求先初始化，然后循环接收客户端数据
        @Override
        public void run() {
            //首先建立客户端与服务端的连接
            InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
            System.out.println("Establish connection: " + address.getAddress().getHostAddress()
                    + ":" + address.getPort());
            Packager packager = null;
            try {
                //然后建立对应这个连接的packager处理之后传进来的数据
                Transporter t = new Transporter(socket);
                Encoder encoder = new Encoder();
                packager = new Packager(t, encoder);
            } catch (IOException ioException) {
                ioException.printStackTrace();
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
            Executor executor = new Executor(tbm);
            while (true) {
                Package pack = null;
                try {
                    //循环遍历取出此次请求中传输的数据
                    pack = packager.receive();
                } catch (Exception e) {
                    break;
                }
                byte[] sql = pack.getData();
                byte[] result = null;
                Exception e = null;
                try {
                    result = executor.execute(sql);
                } catch (Exception exception) {
                    e = exception;
                    e.printStackTrace();
                }
                //将解析的数据封装成网络数据包发送给客户端
                pack = new Package(result, e);
                try {
                    packager.send(pack);
                } catch (Exception exception) {
                    exception.printStackTrace();
                    break;
                }
            }
            //出现异常才会退出循环断开连接
            executor.close();
            try {
                packager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
