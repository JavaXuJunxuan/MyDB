package com.xjx.mydb.server.tbm;

import com.google.common.primitives.Bytes;
import com.xjx.mydb.common.Error;
import com.xjx.mydb.server.im.BPlusTree;
import com.xjx.mydb.server.parser.statement.SingleExpression;
import com.xjx.mydb.server.tm.TransactionManagerImpl;
import com.xjx.mydb.server.utils.Panic;
import com.xjx.mydb.server.utils.ParseStringRes;
import com.xjx.mydb.server.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Xjx
 * @Create: 2023/2/8 - 11:29
 * Field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
public class Field {
    //字段上索引对应的索引二叉树的根节点的uid
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;

    //通过一个字段的uid去对应表中加载出字段信息
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    //根据传入的字段的字节数据解析一个字段信息
    private Field parseSelf(byte[] raw) {
        int position = 0;
        //传入的这个字节数组数据首先是字段名，然后是字段类型。因此需要分段解析
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        //取出字节数组中间的关于字段类型的数据进行解析
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        //解析字段信息中的索引信息
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        //字段可能没有索引，所以判断一下
        if(index != 0) {
            try {
                //把索引对应的二叉树加载进来赋值给这个字段的二叉树属性，bt指向这个索引二叉树，这棵树持有根节点的uid。
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    //根据传入的字段相关参数信息创建一个字段出来
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            //如果字段有索引，就去这个表中穿件一个索引树出来
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    //解析字段类自己为字节数组并传入TBM中为当前表中增加一个当前字段类表示的字段
    //创建一个字段的方法类似，将相关的信息通过 VM 持久化即可，因为TBM基于VM实现，数据都存储在entry中
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    //对字段类型进行类型检查，MyDB中只有下面三种类型
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    //根据一个左右key去查找B树中的数据，这里应该是去查找字段数据，因为字段信息也保存在树中
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    //将传进来的参数值转换成字段对应的类型数据
    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            //uid是long类型的，所以对数据类型进行一下转换
            case "string":
                //字符串则转化成uid
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    //保存字段值的结果
    class ParseValueRes {
        //字段值
        Object v;
        //等于字段长度+字段值的总长，也等于下一个字段值的起始地址
        int shift;
    }

    //根据类型解析字段值
    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                //将字段值解析成字符串，前四字节表示字符串长度
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    //不用将读到的字段值转换为对应类型，因为这里是取出字段值，因此全部转换成字符串即可
    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    //筛选条件中出现比较表达时调用此方法比较字段
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                //小于号表示比较表达式左边是字段
                res.left = 0;
                //将比较值转化成对应的字段值
                v = string2Value(exp.value);
                //然后将字段值转换成可比较的值，如果数值则转化成数值类型，字符串则是比较uid等同于hashcode
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                //小于号表示字段小于某个值即v
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(",")
                .append(fieldType)
                .append(index != 0 ? ",Index":"NoIndex")
                .append(")")
                .toString();
    }

















}
