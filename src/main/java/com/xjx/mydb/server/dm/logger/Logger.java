package com.xjx.mydb.server.dm.logger;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author: Xjx
 * @Create: 2022/12/30 - 11:19
 */
public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    //根据路径创建日志文件，并返回对这个日志文件的日志操作对象
    public static Logger create(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        //因为日志文件前四个字节是固定的日志统计校验和，新建日志则其值为0,
        ByteBuffer buffer = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        return new LoggerImpl(raf, fc, 0);
    }

    //打开指定目录下的日志文件，返回其日志操作对象
    public static Logger open(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        LoggerImpl log = new LoggerImpl(raf, fc);
        log.init();
        return log;
    }
}
