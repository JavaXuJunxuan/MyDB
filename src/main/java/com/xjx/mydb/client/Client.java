package com.xjx.mydb.client;

import com.xjx.mydb.transport.Package;
import com.xjx.mydb.transport.Packager;

/**
 * @Author: Xjx
 * @Create: 2023/2/17 - 13:31
 */
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    //执行SQl语句
    public byte[] execute(byte[] stat) throws Exception {
        //将SQL语句中的数据包装成包格式
        Package pack = new Package(stat, null);
        //收发数据
        Package resPack = rt.roundTrip(pack);
        if(resPack.getErr() != null) {
            throw resPack.getErr();
        }
        return resPack.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {}
    }
}
