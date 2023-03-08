package com.xjx.mydb.client;

import com.xjx.mydb.transport.Encoder;
import com.xjx.mydb.transport.Packager;
import com.xjx.mydb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * @Author: Xjx
 * @Create: 2023/2/18 - 16:54
 */
public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
