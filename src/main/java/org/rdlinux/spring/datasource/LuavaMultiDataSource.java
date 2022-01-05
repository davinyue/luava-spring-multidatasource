package org.rdlinux.spring.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuavaMultiDataSource extends AbstractRoutingDataSource {
    private final static Logger logger = LoggerFactory.getLogger(LuavaMultiDataSource.class);
    private final ThreadLocal<String> contextHolder = new ThreadLocal<>();
    private String masterKey = "master";
    /**
     * 从库keys
     */
    private List<String> slaveDataSourceKeys;

    private int keyIndex = 0;

    public LuavaMultiDataSource(String masterKey, Map<Object, Object> targetDataSources) {
        this.setTargetDataSources(targetDataSources);
        if (masterKey != null && !masterKey.isEmpty()) {
            this.masterKey = masterKey;
        }
    }

    public LuavaMultiDataSource(String masterKey) {
        this(masterKey, null);
    }

    public void initSlaveDataSourceKeys(Map<Object, Object> targetDataSources) {
        if (targetDataSources.get(this.masterKey) == null) {
            throw new IllegalArgumentException(this.masterKey + " datasource can not be null");
        }
        this.slaveDataSourceKeys = new LinkedList<>();
        Set<Object> keys = targetDataSources.keySet();
        for (Object key : keys) {
            if (!this.masterKey.equals(key.toString())) {
                this.slaveDataSourceKeys.add(key.toString());
            }
        }
    }

    @Override
    public void setTargetDataSources(Map<Object, Object> targetDataSources) {
        if (targetDataSources == null || targetDataSources.isEmpty()) {
            throw new IllegalArgumentException("targetDataSources can not be null");
        }
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
        this.contextHolder.set(this.masterKey);
        if (logger.isDebugEnabled()) {
            logger.debug("switch master datasource");
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
                this.contextHolder.set(this.slaveDataSourceKeys.get(this.keyIndex));
                this.keyIndex++;
                if (this.keyIndex >= this.slaveDataSourceKeys.size()) {
                    this.keyIndex = 0;
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("switch {} datasource", this.contextHolder.get());
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
        if (!this.slaveDataSourceKeys.contains(key)) {
            key = this.masterKey;
        }
        this.contextHolder.set(key);
        if (logger.isDebugEnabled()) {
            logger.debug("switch {} datasource", key);
        }
    }

    public String getDataSourceKey() {
        String key = this.contextHolder.get();
        if (key == null) {
            key = this.masterKey;
            this.markMaster();
        }
        return key;
    }
}
