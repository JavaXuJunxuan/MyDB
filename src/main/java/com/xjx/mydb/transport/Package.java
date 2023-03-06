package com.xjx.mydb.transport;

/**
 * @Author: Xjx
 * @Create: 2023/2/14 - 12:54
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
