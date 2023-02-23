package com.xjx.mydb.server.dm.logger;

import org.junit.Test;

import java.io.File;

/**
 * @Author: Xjx
 * @Create: 2023/1/7 - 20:07
 */
public class LoggerTest {
    @Test
    public void testLogger() {
        Logger lg = Logger.create("D:\\JavaProject\\MyDB\\tmp\\logger_test");
        lg.log("aaaaaa".getBytes());
        lg.log("bbbbbb".getBytes());
        lg.log("cccccc".getBytes());
        lg.log("dddddd".getBytes());
        lg.log("eeeeee".getBytes());
        lg.close();

        lg = Logger.open("D:\\JavaProject\\MyDB\\tmp\\logger_test");
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaaaaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbbbbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "cccccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "dddddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eeeeee".equals(new String(log));

        lg.close();

        assert new File("D:\\JavaProject\\MyDB\\tmp\\logger_test.log").delete();
    }
}
