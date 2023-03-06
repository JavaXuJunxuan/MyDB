package com.xjx.mydb.transport;

/**
 * @Author: Xjx
 * @Create: 2023/2/14 - 14:21
 */
public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    //将封装好的数据发送出去
    public void send(Package pack) throws Exception {
        //首先将对应数据根据数据传输格式编码成字节数组
        byte[] data = encoder.encode(pack);
        //利用封装好的运输类将数据发送出去，发送时候会对数据格式转换一下，便于输入输出流读取输入
        transporter.send(data);
    }

    //将发送来的数据接收，并封装为可处理的package对象
    public Package receive() throws Exception {
        //接收的字符串然后转变为字节数组
        byte[] data = transporter.receive();
        //然后对字节数组再解码为对象
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transporter.close();
    }
}
