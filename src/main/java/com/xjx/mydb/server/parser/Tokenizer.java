package com.xjx.mydb.server.parser;

import com.xjx.mydb.common.Error;

/**
 * @Author: Xjx
 * @Create: 2023/2/4 - 13:30
 */
public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    //当前遍历出的token是否可以从分词器中取出来，即当前分词器是否完整遍历了一个token
    private boolean flushToken;
    private Exception err;

    //对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个 token
    //将要解析的类SQL语句的字节数组传入
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    //取出分词器最新遍历的token
    public String peek() throws Exception {
        //有异常直接抛异常
        if(err != null) {
            throw err;
        }
        //只有true才可以取出token，true的意思是分词器已经遍历好了一个完整token
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch (Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            //取出一个完整token后要改为false，避免下次取token出现错误
            flushToken = false;
        }
        return currentToken;
    }

    //将是否可取token标志位改为true
    public void pop() {
        flushToken = true;
    }

    //返回下一个将要遍历的token
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    //返回下一个元数据
    private String nextMetaState() throws Exception {
        //循环的目的就是找到第一个非空白符字节
        while (true) {
            //取出当前pos所在位置的字节数据
            Byte b = peekByte();
            //==null表示当前stat遍历完了，没有token了
            if(b == null) {
                return "";
            }
            //当前遍历字节不是空白符则退出循环
            if(!isBlank(b)) {
                break;
            }
            //向后接着遍历
            popByte();
        }
        //取出第一个非空白符字节数据
        byte b = peekByte();
        //判断是否是一个符号字节
        if(isSymbol(b)) {
            //是则向后遍历一位，然后以字符串形式把这个符号字节返回
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            //是否是一个引号，即判断之后连着的是关键字还是字符串
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            //都不是则出错
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    //遍历出下一个数值token或者关键字token
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while (true) {
            //取出下一个要遍历的字节数据
            Byte b = peekByte();
            //只有要取出的字节为null即遍历完sql了或者当前遍历的字节既不是字母也不是数字也不是_时才会进入if体
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                //b不为null且为空白符则pos向后一位
                if(b != null && isBlank(b)) {
                    popByte();
                }
                //返回之前遍历的token
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    //取出下一个字符串数值token
    private String nextQuoteState() throws Exception {
        //将当前pos指向的引号字符去掉
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            //当取到字符串结尾的引号就可以退出循环了，同时pos下标后移
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    //出现错误时将错误信息加到stat中
    public byte[] errStat() {
        //创建一个大小等于stat长度+3的字节数组
        byte[] res = new byte[stat.length + 3];
        //将当前pos之前遍历完的字节赋值到新数组中
        System.arraycopy(stat, 0, res, 0, pos);
        //记录一个”<<”字符标志位到出异常位置
        System.arraycopy("<<".getBytes(), 0, res, pos, 3);
        //将尚未遍历到的字节数据加到新数组中
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        return res;
    }

    //pos下标+1，表示向后继续遍历一位，如果超出stat范围则=stat长度
    private void popByte() {
        pos++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    //返回当前pos所在下标的stat字节数组的字节
    private Byte peekByte() {
        //如果当前分词器解析位置等于stat中语句长度则没有可解析token
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    //判断当前字节是否是一个数字
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    //判断当前字节是否是一个字母
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    //判断当前字节是否是一个符号位
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' || b == ',' ||
                b == '(' || b == ')');
    }

    //判断当前字节是否是一个空白符即换行符，空格或者制表符
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
