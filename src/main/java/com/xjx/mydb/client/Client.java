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

    public byte[] execute(byte[] stat) throws Exception {
        Package pack = new Package(stat, null);
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
