package com.xjx.mydb.server.tbm;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @Author: Xjx
 * @Create: 2023/2/11 - 9:14
 */
public class Booter {
    //启动文件的后缀
    public static final String BOOTER_SUFFIX = ".bt";
    //修改启动文件的临时文件后缀
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        return new Booter(path, f);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        return buf;
    }

    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException ioException) {
            Panic.panic(ioException);
        }
        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        file = new File(path + BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWExcepiton);
        }
    }
}
