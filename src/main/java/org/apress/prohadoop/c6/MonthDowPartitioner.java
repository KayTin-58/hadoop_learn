package org.apress.prohadoop.c6;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apress.prohadoop.utils.AirlineDataUtils;

/**
 * 关键类 实现全排序
 * 自己建立算法  对所有的键进行一个划分  保证一定的键区间在每个Reducer
 * 中有序  并且所有的Reducer又是有序的
 *
 * 实现 Configurable 接口 为了方便可以动态获取 Reducer任务的数量
 * Created by zb on 2018/12/10.
 */
public class MonthDowPartitioner extends Partitioner<MonthDoWWritable, Text>
        implements Configurable {

    private Configuration conf = null;
    private int indexRange = 0;

    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.indexRange = conf.getInt("range", getDefaultRange());
    }

    public Configuration getConf() {
        return this.conf;
    }


    /**
     * 实现全局排序的关键算法
     *
     * @param monthDoWWritable
     * @param text
     * @param numReduce
     * @return
     */
    @Override
    public int getPartition(MonthDoWWritable monthDoWWritable, Text text, int numReduce) {
        return AirlineDataUtils.getCustomPartition(monthDoWWritable, indexRange, numReduce);
    }


    public int getDefaultRange() {
        int minRange = 0;
        int maxRange = 7 * 11 + 6;
        int defaultRange = (maxRange - minRange) + 1;
        return defaultRange;
    }
}
