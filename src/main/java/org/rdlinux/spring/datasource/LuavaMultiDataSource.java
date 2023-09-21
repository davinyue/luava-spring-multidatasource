package org.rdlinux.spring.datasource;

import org.rdlinux.spring.datasource.event.SwitchDataSourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuavaMultiDataSource extends AbstractRoutingDataSource implements ApplicationListener<SwitchDataSourceEvent> {
    public static final String DEFAULT_MASTER_KEY = "master";
    private final static Logger log = LoggerFactory.getLogger(LuavaMultiDataSource.class);
    private final ThreadLocal<String> CURRENT_DATASOURCE_KEY_TL = new ThreadLocal<>();
    private String masterKey;
    /**
     * 从库keys
     */
    private List<String> slaveDataSourceKeys;
    /**
     * 当前从库使用下标
     */
    private volatile int keyIndex = 0;

    /**
     * 构造函数
     *
     * @param masterKey         指定主数据源的名称
     * @param targetDataSources 指定数据源名称与数据源的映射
     */
    @SuppressWarnings("unchecked")
    public LuavaMultiDataSource(String masterKey, Map<String, DataSource> targetDataSources) {
        if (masterKey != null && !masterKey.isEmpty()) {
            this.masterKey = masterKey;
        }
        this.setTargetDataSources((Map<Object, Object>) ((Map<?, ?>) targetDataSources));
    }

    /**
     * 构造函数
     *
     * @param targetDataSources 指定数据源名称与数据源的映射
     */
    public LuavaMultiDataSource(Map<String, DataSource> targetDataSources) {
        this(DEFAULT_MASTER_KEY, targetDataSources);
    }

    /**
     * 初始化从库
     */
    public void initSlaveDataSourceKeys(Map<Object, Object> targetDataSources) {
        if (targetDataSources.get(this.masterKey) == null) {
            throw new IllegalArgumentException(this.masterKey + " datasource can not be null");
        }
        Set<Object> keys = targetDataSources.keySet();
        int salveNum = keys.size() - 1;
        if (salveNum > 0) {
            this.slaveDataSourceKeys = new ArrayList<>(salveNum);
            for (Object key : keys) {
                if (!this.masterKey.equals(key.toString())) {
                    this.slaveDataSourceKeys.add(key.toString());
                }
            }
        }
    }

    @Override
    public void setTargetDataSources(Map<Object, Object> targetDataSources) {
        Assert.notEmpty(targetDataSources, "targetDataSources can not be null");
        super.setTargetDataSources(targetDataSources);
        this.initSlaveDataSourceKeys(targetDataSources);
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return this.getDataSourceKey();
    }

    /**
     * 标记主库
     */
    public void markMaster() {
        this.CURRENT_DATASOURCE_KEY_TL.set(this.masterKey);
        if (log.isDebugEnabled()) {
            log.debug("switch master datasource");
        }
    }

    /**
     * 标记从库
     */
    public void markSlave() {
        if (this.slaveDataSourceKeys == null || this.slaveDataSourceKeys.isEmpty()) {
            this.markMaster();
        } else {
            synchronized (this) {
                this.CURRENT_DATASOURCE_KEY_TL.set(this.slaveDataSourceKeys.get(this.keyIndex));
                this.keyIndex++;
                if (this.keyIndex >= this.slaveDataSourceKeys.size()) {
                    this.keyIndex = 0;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("switch {} datasource", this.CURRENT_DATASOURCE_KEY_TL.get());
            }
        }
    }

    /**
     * 根据key标记库
     */
    public void markByKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key can not be null");
        }
        if (this.slaveDataSourceKeys == null || !this.slaveDataSourceKeys.contains(key)) {
            key = this.masterKey;
        }
        this.CURRENT_DATASOURCE_KEY_TL.set(key);
        if (log.isDebugEnabled()) {
            log.debug("switch {} datasource", key);
        }
    }

    public String getDataSourceKey() {
        String key = this.CURRENT_DATASOURCE_KEY_TL.get();
        if (key == null) {
            key = this.masterKey;
            this.markMaster();
        }
        return key;
    }

    @Override
    public void onApplicationEvent(SwitchDataSourceEvent event) {
        this.markByKey(event.getSource());
    }
}
