package com.xjx.mydb.transport;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.common.Error;

import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2023/2/14 - 13:34
 */
public class Encoder {

    //将传输数据的对象编码成字节数组来传输
    public byte[] encode(Package pack) {
        //有异常则要处理异常信息
        if(pack.getErr() != null) {
            Exception err = pack.getErr();
            String msg = "Intern server error";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pack.getData());
        }
    }

    //将接收的字节数组形式的数据解码成MyDB服务端可以处理的对象
    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        //数据第一个字节是标志位，0正常1异常。对应data为正常数据和异常数据
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }
}
