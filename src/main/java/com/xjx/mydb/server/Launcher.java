package com.xjx.mydb.server;

import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.tbm.TableManager;
import com.xjx.mydb.server.tm.TransactionManager;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.vm.VersionManager;
import com.xjx.mydb.server.vm.VersionManagerImpl;
import org.apache.commons.cli.*;

/**
 * @Author: Xjx
 * @Create: 2023/2/19 - 10:15
 */
public class Launcher {
    public static final int port = 9999;
    public static final long DEFAULT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws Exception {
        //启动服务器时先定义三种开启数据库的属性信息
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        //然后开启一个命令行解析器解析这个类启动时的配置
        CommandLineParser parser = new DefaultParser();
        //将当前方法参数和之前定义的选项一起解析
        CommandLine cmd = parser.parse(options, args);
        //判断方法参数有哪些以此来看是新建数据库文件还是打开某个建好的数据库
        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    //根据路径创建一个数据库文件
    private static void createDB(String path) throws Exception {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * KB;
            case "GB":
                return memNum * KB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;
    }
}
