package com.xjx.mydb.client;

import java.util.Scanner;

/**
 * @Author: Xjx
 * @Create: 2023/2/18 - 10:41
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    //就是用来读取控制台上用户的输入信息的
    public void run() {
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.println(":> ");
                String statStr = scanner.nextLine();
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    //接收用户命令行写入的SQL语句然后执行
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            scanner.close();
            client.close();
        }
    }

}
