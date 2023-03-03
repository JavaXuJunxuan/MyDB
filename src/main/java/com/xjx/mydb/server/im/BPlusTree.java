package com.xjx.mydb.server.im;

import com.xjx.mydb.server.common.SubArray;
import com.xjx.mydb.server.dm.DataManager;
import com.xjx.mydb.server.dm.dataItem.DataItem;
import com.xjx.mydb.server.tm.TransactionManagerImpl;
import com.xjx.mydb.server.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Xjx
 * @Create: 2023/1/29 - 11:07
 */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    //由于B+树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem
    //该 DataItem 中存储了根节点的 UID。可以注意到，IM 在操作 DM 时，使用的事务都是 SUPER_XID。
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        //生成一个空根节点
        byte[] rawRoot = Node.newNilRootRaw();
        //将生成的节点数据插入数据管理器DM中，这个插入的是节点数据
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        //下面这个插入的根节点的uid即节点索引，返回的是索引的uid即页号加偏移量，可以通过此uid找到根节点数据的索引地址
        //然后根据索引找数据
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    //根据一棵树的根节点id加载一棵树
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    //获取根节点uid
    private long rootUid() {
        bootLock.lock();
        try {
            //获取B树中保存的根节点的引用指向的根节点数据项的数据
            SubArray sa = bootDataItem.data();
            //取出数据项中的uid，因为根节点指向的Dataitem是一个索引数据项，保存的是存储真正根节点数据项的uid
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    //更新根节点uid
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //这里将刚生成的根节点数据插入dm中，返回的是新根节点的uid
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            //下面涉及更新操作，所以需要保存旧数据
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            //将刚才新根节点的uid插入B树中的对根节点的引用中，即更新根节点对应数据项中存储的uid，此uid对应的数据项保存真正的根节点数据
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            //执行到此处表示更新成功，记录更新后的新数据，保证原子性
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    //根据节点uid和要查找数据对应的键找存储该key的叶子节点，不是叶子节点也可能存在该key，因为索引节点也会存储相应的索引key
    private long searchLeaf(long nodeUid, long key) throws Exception {
        //根据要找的节点uid找节点
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        if (isLeaf) {
            //如果当前这个根据uid找的节点是叶子节点就直接返回
            return nodeUid;
        } else {
            //不是叶子节点则找下一个节点即当前节点为索引节点,这就是为什么方法传入一个uid一个key了。因为dm中数据项是随机存的，前后数据项没有逻辑关系
            //所以一旦uid找不到就要根据传入的key根据索引找key对应的节点
            long next = searchNext(nodeUid, key);
            //然后对下一个节点找叶子节点
            return searchLeaf(next, key);
        }
    }

    //找当前节点的下一个节点
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            //将当前节点取出
            Node node = Node.loadNode(this, nodeUid);
            //根据key去当前节点找key对应的下个节点地址，即索引查找
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            //不等于0则找到了
            if(res.uid != 0) return res.uid;
            //等于则返回其兄弟节点id
            nodeUid = res.siblingUid;
        }
    }

    //根据key在节点中找key对应的数据
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    //根据左右key边界范围查找
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        //找到叶子节点，因为数据保存在叶子节点中
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            //如果遍历的叶子节点中为遍历完就已经不符合遍历条件则不会有兄弟节点即查找完毕
            if(res.siblingUid == 0) {
                break;
            } else {
                //如果兄弟节点不为0，则表示上面查找key对应的数据时将所查叶子节点遍历完仍符合key要求，则还需要到兄弟节点查
                leafUid = res.siblingUid;
            }
        }
        //返回查找到的数据项的uid
        return uids;
    }

    //向当前树中插入数据即数据id和key
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    //插入key的返回结果
    class InsertRes {
        long newNode, newKey;
    }

    //向nodeUid对应的节点插入新节点地址和其对应的键，即索引表中操作
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        //加载被插入数据的节点
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        InsertRes res = null;
        //如果是叶子节点则直接插入数据
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            //不是叶子节点则往非叶子节点也插入数据，作为索引用
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(nodeUid, uid, key);
            //判断非叶子节点是否分裂
            if(ir.newNode != 0) {
                //分裂了则需要插入新分裂出的节点
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                //没分裂则直接返回
                res = new InsertRes();
            }
        }
        return res;
    }

    //向当前节点插入新节点id和key，索引表操作
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            //插入数据没有成功，nodeUid对应节点没有key的位置，所以nodeUid=兄弟节点，继续遍历其兄弟节点找插入位置
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                //插入成功
                InsertRes res = new InsertRes();
                //只有在插入数据之后节点分裂了，下面的newSon/Key才有值，没分裂则无值
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
