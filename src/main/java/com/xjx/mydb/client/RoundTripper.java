package com.xjx.mydb.client;

import com.xjx.mydb.transport.Package;
import com.xjx.mydb.transport.Packager;

/**
 * @Author: Xjx
 * @Create: 2023/2/17 - 10:14
 */
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    //收发数据
    public Package roundTrip(Package pack) throws Exception {
        packager.send(pack);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
