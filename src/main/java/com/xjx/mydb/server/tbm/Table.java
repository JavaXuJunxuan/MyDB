package com.xjx.mydb.server.tbm;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.parser.statement.*;
import com.xjx.mydb.server.tm.TransactionManagerImpl;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.ParseStringRes;
import com.xjx.mydb.server.utils.Parser;

import java.util.*;

/**
 * @Author: Xjx
 * @Create: 2023/2/9 - 10:34
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    //每张表都有一个uid
    long uid;
    String name;
    byte status;
    //每张表都持有下一个表的uid，因为TBM通过链表将表组织起来
    long nextUid;
    //维护了这张表中所有的字段结构
    List<Field> fields = new ArrayList<>();

    //通过TBM和表的uid将表数据加载进来并解析为表结构
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    //根据建表语句创建出一个表出来
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        //遍历建表语句中的字段
        for(int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            //遍历一个字段就遍历索引
            for(int j = 0; j < create.index.length; j++) {
                //如果索引名称等于字段名则此字段有索引
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            //将遍历到的字段及其索引信息加入表结构中的字段集合中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }
        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }


    //解析一段表数据（格式在开头）
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    //根据当前表中属性解析出一个表
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    //根据删除token中的信息，将表中对应的字段信息删除掉，同时需要记录事务id
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for(Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    //根据更新token中的信息，将表中对应的字段信息进行更新，同时需要记录事务id
    public int update(long xid, Update update) throws Exception {
        //找到所有符合更新要求的字段id
        List<Long> uids = parseWhere(update.where);
        Field field = null;
        //遍历当前类中的字段属性找到要进行修改的那个字段
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                field = f;
                break;
            }
        }
        if(field == null) {
            throw Error.FieldNotFoundException;
        }
        //取出新值
        Object value = field.string2Value(update.value);
        int count = 0;
        for(Long uid : uids) {
            //根据记录的id读出记录数据
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //将旧记录删除
            ((TableManagerImpl)tbm).vm.delete(xid, uid);
            Map<String, Object> entry = parseEntry(raw);
            entry.put(field.fieldName, value);
            raw = entry2Raw(entry);
            //插入新纪录即为更新数据
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
            count++;
            //然后找出带索引的字段，将对应字段记录上索引属性
            for(Field fd : fields) {
                if(fd.isIndexed()) {
                    fd.insert(entry.get(fd.fieldName), uuid);
                }
            }
        }
        return count;
    }

    //根据查询语句中的表明字段和过滤条件读数据
    public String read(long xid, Select read) throws Exception {
        //解析where语句找出符合条件的记录
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        //读取记录
        for(Long uid : uids) {
            //去表中读取记录
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //然后根据记录格式转换成集合
            Map<String, Object> entry = parseEntry(raw);
            //然后输出
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    //将插入数据解析对应字段形式
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        //将遍历出的字段集合解析成记录行格式
        byte[] raw = entry2Raw(entry);
        //将记录行插入表中
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        //然后找出有索引的字段，将字段信息插入字段中
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    //将记录中保存字段值的数组解析成记录形式
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        //这里保存字段的数组大小要等于表中记录的字段总大小即等于字段总数，因为MyDB只实现了全字段插入数据
        if(values.length != fields.size()) {
            throw Error.InvalidCommandException;
        }
        Map<String, Object> entry = new HashMap<>();
        for(int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    //解析where语句
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;
        //过滤条件为null则看看那个字段有索引，因为MyDB只能靠索引查找。
        if(where == null) {
            for(Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
        } else {
            //where不为null则表示有过滤条件，则此时where中的语句必有索引，因为MyDB只能支持索引查找
            for(Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotFoundException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            //根据where过滤字段解析where语句，然后取出左右边界
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }
        //根据解析出来的要过滤字段的左右边界查询对应记录的uid
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        //是否只有一次过滤，or有两次
        boolean single;
    }

    private CalWhereRes calWhere(Field field, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = field.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                //or需要过滤两次
                res.single = false;
                //解析where条件中的表达式
                r = field.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = field.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = field.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = field.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                //结果集中左边要最大的，右边要最小的
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidCommandException;
        }
        return res;
    }

    //将一行记录的字段值打印出来
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for(int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    //将一行记录中的字段信息都取出来
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for(Field field : fields) {
            Field.ParseValueRes res = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, res.v);
            pos += res.shift;
        }
        return entry;
    }

    //将一个记录中的所有字段值取出来
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        //fields是表对象维护的一个字段集，里面保存了当前表对象代表的那个表结构中的字段结构
        for(Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        //表名
        sb.append(name).append(":");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(",");
            }
        }
        return sb.toString();
    }

}
