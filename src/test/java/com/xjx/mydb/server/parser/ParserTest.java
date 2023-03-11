package com.xjx.mydb.server.parser;

import com.google.gson.Gson;
import com.xjx.mydb.server.parser.statement.*;
import org.junit.Test;
import java.util.Arrays;

/**
 * @Author: Xjx
 * @Create: 2023/2/23 - 10:12
 */
public class ParserTest {
    @Test
    public void testCreate() throws Exception {
        String stat = "create table student id int32, name string, uid int64, (index name id uid)";
        Object res = Parser.parse(stat.getBytes());
        Create create = (Create)res;
        assert "student".equals(create.tableName);
        System.out.println("Create");
        for (int i = 0; i < create.fieldName.length; i++) {
            System.out.println(create.fieldName[i] + ":" + create.fieldType[i]);
        }
        System.out.println(Arrays.toString(create.index));
        System.out.println("======================");
    }

    @Test
    public void testBegin() throws Exception {
        String stat = "begin isolation level read committed";
        Object res = Parser.parse(stat.getBytes());
        Begin begin = (Begin)res;
        assert !begin.isRepeatableRead;

        stat = "begin";
        res = Parser.parse(stat.getBytes());
        begin = (Begin)res;
        assert !begin.isRepeatableRead;

        stat = "begin isolation level repeatable read";
        res = Parser.parse(stat.getBytes());
        begin = (Begin)res;
        assert begin.isRepeatableRead;
    }

    @Test
    public void testRead() throws Exception {
        String stat = "select name, id, strudeng from student where id > 1 and id < 4";
        Object res = Parser.parse(stat.getBytes());
        Select select = (Select)res;
        assert "student".equals(select.tableName);
        Gson gson = new Gson();
        System.out.println("Select");
        System.out.println(gson.toJson(select.fields));
        System.out.println(gson.toJson(select.where));
        System.out.println("======================");
    }

    @Test
    public void testInsert() throws Exception {
        String stat = "insert into student values 5 \"Guo Ziyang\" 22";
        Object res = Parser.parse(stat.getBytes());
        Insert insert = (Insert)res;
        Gson gson = new Gson();
        System.out.println("Insert");
        System.out.println(gson.toJson(insert));
        System.out.println("======================");
    }

    @Test
    public void testDelete() throws Exception {
        String stat = "delete from student where name = \"Guo Ziyang\"";
        Object res = Parser.parse(stat.getBytes());
        Delete delete = (Delete)res;
        Gson gson = new Gson();
        System.out.println("Delete");
        System.out.println(gson.toJson(delete));
        System.out.println("======================");
    }

    @Test
    public void testShow() throws Exception {
        String stat = "show";
        Object res = Parser.parse(stat.getBytes());
        Show show = (Show)res;
        Gson gson = new Gson();
        System.out.println("Show");
        System.out.println(gson.toJson(show));
        System.out.println("======================");
    }

    @Test
    public void testUpdate() throws Exception {
        String stat = "update student set name = \"GZY\" where id = 5";
        Object res = Parser.parse(stat.getBytes());
        Update update = (Update)res;
        Gson gson = new Gson();
        System.out.println("Update");
        System.out.println(gson.toJson(update));
        System.out.println("======================");
    }
}
