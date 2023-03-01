package com.xjx.mydb.server.im;

import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.tm.TransactionManagerImpl;
import com.xjx.mydb.server.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Xjx
 * @Create: 2023/1/24 - 14:42
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    //0下标处为叶子节点的标志位
    static final int IS_LEAF_OFFSET = 0;
    //1下标处为该节点中key的个数,占两个字节
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    //3下标处为该节点的兄弟节点存储在DM的UID
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    //每个节点的头大小
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
    //平衡因子，非根节点最小key数量
    static final int BALANCE_NUMBER = 32;
    //节点的最大内存大小
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    //节点有自己所属的那颗树的引用
    BPlusTree tree;
    //node存储在DataItem中
    DataItem dataItem;
    //节点的数据，实际是和数据项中data共用的
    SubArray raw;
    //节点对应数据的uid地址
    long uid;

    //设置一个节点是否为叶子节点
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    //判断一个节点是否是叶子节点
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    //修改一个节点中key的总数
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    //查询一个节点中key总数
    static int getRawNoKey(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    //设置一个节点的兄弟节点
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    //查询一个节点的兄弟节点
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    //设置该节点的第k个键对应的子节点地址
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    //得到该节点的第k个键对应的子节点地址
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    //设置该节点的第k键
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    //得到该节点的第k键,kth=0即为第一个键，因为随机存取
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    //将从第kth开始的节点全部复制到另一个树上，这里kth=0是第一个节点，32则为第33个节点
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    //从某个指定键的节点开始（包括此节点）将节点都向后移动一个节点，此方法用于插入结点时方便插入
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    //生成一个根节点
    static byte[] newRootRaw(long left, long right, long key) {
        //生成一个等于节点大小的数组
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //设置为非子节点
        setRawIsLeaf(raw, false);
        //新根节点默认有两个初始左右子节点，所以节点个数为2
        setRawNoKeys(raw, 2);
        //根节点没有兄弟节点
        setRawSibling(raw, 0);
        //设置左节点
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        //设置右节点
        setRawKthSon(raw, right, 1);
        //当前右节点即为最后一个节点，所以值为Max_Value
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.raw;
    }

    //生成一个空的根节点数据
    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }

    //从一棵树中根据记录的uid取出这个节点，但因为每个node都存在数据项中，所以只要知道数据项然后赋值引用就可以了
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    //因为node存在DataItem里，所以释放节点缓存就是释放对应数据项的缓存
    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnlock();
        }
    }

    //为了查找方便，创建一个查找结点类
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    //寻找传入的key的下一个键及其数据
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            //得到当前节点中的总key数，然后遍历
            int noKeys = getRawNoKey(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                //找到第一个节点中键大小大于传入的key的就是此key下一个键，返回其对应数据
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            //找不到则返回兄弟节点
            res.uid = 0;
            //节点的兄弟节点记录在此此节点中
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnlock();
        }
    }

    //范围查找结点类，找到的所有符合要求的键对应的数据都放入这个类的对象中
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    //在当前节点进行范围查找，范围是 [leftKey, rightKey]
    //这里约定如果rightKey大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            //获得节点的总键数
            int noKeys = getRawNoKey(raw);
            int kth = 0;
            //遍历节点
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                //因为是范围查找，找到左边界就先退出，因为这里的键对应的数据就符合我们的要求需要收集起来
                if (leftKey <= ik) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                //从左边界一直遍历，直到不符合要求的键退出
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            //如果当前节点所有键都查完了，说明兄弟节点可能也有符合范围的答案，把兄弟节点也返回
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            //返回范围查找结果对象
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnlock();
        }
    }

    //插入数据的节点对象，保存兄弟节点，新节点地址即uid，新key
    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    //节点中插入键时当前键可能存在其他键了，所以需要后移留出位置供插入
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        //执行插入操作之前的准备工作
        boolean success = false;
        Exception err = null;
        //这个res中有三个属性，只有在插入不成功返回兄弟节点，或者插入成功但是需要分裂的情况下res中属性才会被赋值
        InsertAndSplitRes res = new InsertAndSplitRes();
        //把旧数据保存起来，这里因为记录都是以数据项为单位的，所有把节点对应的数据项保存起来
        dataItem.before();
        try {
            //根据新插入数据的键找到其位置并插入
            success = insert(uid, key);
            //失败则说明当前节点没有插入位置，返回其兄弟节点
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            //插入完节点后要判断节点存的key是否达到上限，达到了则需要分裂节点
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            //插入成功，需要记录redo日志
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                //插入失败，撤回之前的修改旧数据操作
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKey(raw);
        int kth = 0;
        //遍历节点所有键找到第一个可插入位置
        while (kth < noKeys) {
            //根据传入的要插入的节点的key进行比对找
            long ik = getRawKthKey(raw, kth);
            //大于则当前遍历的键则不是
            if (ik < key) {
                kth++;
            } else {
                //小于等于则是，因为可能中间键差较大，我们找的是第一个可插入位置
                break;
            }
        }
        //查遍整个节点都没有找到可插入位置并且这个节点有兄弟节点返回插入失败
        if (kth == noKeys && getRawSibling(raw) != 0) return false;
        //执行到这里表示找到了可插入位置
        //判断当前节点是否叶子节点
        if (getRawIfLeaf(raw)) {
            //将当前位置及其后面的节点都后移，空出要插入的位置
            shiftRawKth(raw, kth);
            //将要插入的节点赋值到这个空出的待插入位置
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            //非叶的逻辑感觉和叶差不多，只不过叶子节点把键对应的uid放在键左侧（正确）非叶放在右侧
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    //一旦当前节点的键达到了64个就需要分裂节点了
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKey(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    //分裂节点
    private SplitRes split() throws Exception {
        //创建一个和节点大小相等的共享数组
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //将当前节点的数据转移到这个共享数组里
        //首先是节点的是否叶节点的标志信息
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        //然后设置分裂后节点的键数，因为分裂节点是一分为2，所以键数=平衡因子
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        //设置分裂之后的兄弟节点即原节点的兄弟节点
        setRawSibling(nodeRaw, getRawSibling(raw));
        //将当前节点第32个键之后的键分裂到新建的节点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        //将这个新分裂出的节点插入到dm中，让dm管理，因为节点本质上还是数据项DataItem
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        //修改分裂后的当前节点中键数为32
        setRawNoKeys(raw, BALANCE_NUMBER);
        //设置当前节点的新兄弟节点的地址，即新建的分裂节点
        setRawSibling(raw, son);
        //返回分裂后的新建节点的uid以及这个节点的第一个key对应的数据
        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    //以字符串形式返回节点信息
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKey(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}

