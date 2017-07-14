package com.github.talefairy.flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.util.LinkedList;
import java.util.List;

/**
 * 用于序列化头部和主体的事件序列化器  将它们写入hbase的事件
 * Created by supoman on 2017/7/13.
 */
public class SimpleHbaseEventSerializer implements HbaseCustomEventSerializer {

    private String rowPrefix;
    private byte[] incrementRow;
    private byte[] cf;
    private byte[] plCol;
    private byte[] incCol;
    private KeyType keyType;
    private byte[] payload;

    public SimpleHbaseEventSerializer() {

    }

    @Override
    public void configure(Context context) {
        //获取RowKey的前缀，固定的部分，默认前缀是default
        rowPrefix = context.getString("rowPrefix", "default");
        //获取计数器对应的行键
        incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
        //rowkey的类型(可以指定的有四种uuid/random/timestamp/nano)，默认是uuid
        String suffix = context.getString("suffix", "custom");
        //要写入HBase的列名
        String payloadColumn = context.getString("payloadColumn", "pCol");
        //计数器对应的列
        String incColumn = context.getString("incrementColumn", "iCol");
        //根据suffix决定rowkey类型
        if (payloadColumn != null && !payloadColumn.isEmpty()) {
//            if (suffix.equals("timestamp")) {
//                keyType = KeyType.TS;
//            } else if (suffix.equals("random")) {
//                keyType = KeyType.RANDOM;
//            } else if (suffix.equals("nano")) {
//                keyType = KeyType.TSNANO;
//            } else if (suffix.equals("uuid")) {
//                keyType = KeyType.UUID;
//            } else {
//                // 自定义keyType类型
//                keyType = KeyType.CUSTOM_KEY;
//            }

            //列名
            plCol = payloadColumn.getBytes(Charsets.UTF_8);
        }

        //存在计数器列
        if (incColumn != null && !incColumn.isEmpty()) {
            incCol = incColumn.getBytes(Charsets.UTF_8);
        }
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }

    /**
     *
     * @param event Event to be written to HBase 获取要处理的数据
     * @param cf 获取要写入的列簇
     */
    @Override
    public void initialize(Event event, byte[] cf) {
        this.payload = event.getBody();
        this.cf = cf;
    }

    /**
     * 根据keyType 生成不同的rowKey 并Put
     * @return  List<Row>
     */
    @Override
    public List<Row> getActions() {
        List<Row> actions = new LinkedList<Row>();
        if (plCol != null) {
            byte[] rowKey;
            try {
//                if (keyType == KeyType.TS) {
//                    rowKey = SimpleRowKeyGenerator.getTimestampKey(rowPrefix);
//                } else if (keyType == KeyType.RANDOM) {
//                    rowKey = SimpleRowKeyGenerator.getRandomKey(rowPrefix);
//                } else if (keyType == KeyType.TSNANO) {
//                    rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowPrefix);
//                } else if (keyType == KeyType.UUID) {
//                    rowKey = SimpleRowKeyGenerator.getUUIDKey(rowPrefix);
//                } else {
                    // 自定义rowKey
                    rowKey = SimpleRowKeyGenerator.getKey(rowPrefix, payload);
//                }

                Put put = new Put(rowKey);
                put.add(cf, plCol, payload);
                actions.add(put);
            } catch (Exception e) {
                throw new FlumeException("Could not get row key!", e);
            }

        }
        return actions;
    }

    @Override
    public List<Increment> getIncrements() {
        List<Increment> incs = new LinkedList<Increment>();
        if (incCol != null) {
            Increment inc = new Increment(incrementRow);
            inc.addColumn(cf, incCol, 1);
            incs.add(inc);
        }
        return incs;
    }

    @Override
    public void close() {

    }

    /**
     * 枚举常量
     */
    public enum KeyType {
        UUID,
        RANDOM,
        TS,
        TSNANO,
        CUSTOM_KEY
    }

}
