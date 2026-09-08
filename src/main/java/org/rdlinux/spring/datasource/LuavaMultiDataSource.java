package org.rdlinux.spring.datasource;

import org.rdlinux.spring.datasource.event.SwitchDataSourceEvent;
import org.rdlinux.spring.datasource.event.SwitchDataSourceContainsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 支持在多个数据源之间进行线程级路由切换的数据源。
 */
public class LuavaMultiDataSource extends AbstractRoutingDataSource implements ApplicationListener<SwitchDataSourceEvent> {
    /**
     * 默认主数据源 key。
     */
    public static final String DEFAULT_MASTER_KEY = "master";
    /**
     * 日志记录器。
     */
    private final static Logger log = LoggerFactory.getLogger(LuavaMultiDataSource.class);
    /**
     * 当前线程绑定的数据源 key。
     */
    private final ThreadLocal<String> CURRENT_DATASOURCE_KEY_TL = new ThreadLocal<>();
    /**
     * 主数据源 key。
     */
    private String masterKey;
    /**
     * 从库数据源 key 列表。
     */
    private volatile List<String> slaveDataSourceKeys;
    /**
     * 当前从库使用下标。
     */
    private volatile int keyIndex = 0;

    /**
     * 使用指定主数据源 key 和数据源映射创建多数据源路由器。
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
     * 使用默认主数据源 key 创建多数据源路由器。
     *
     * @param targetDataSources 指定数据源名称与数据源的映射
     */
    public LuavaMultiDataSource(Map<String, DataSource> targetDataSources) {
        this(DEFAULT_MASTER_KEY, targetDataSources);
    }

    /**
     * 根据数据源映射初始化从库 key 列表。
     *
     * @param targetDataSources 数据源名称与数据源的映射
     */
    public void initSlaveDataSourceKeys(@NonNull Map<Object, Object> targetDataSources) {
        if (targetDataSources.get(this.masterKey) == null) {
            throw new IllegalArgumentException(this.masterKey + " datasource can not be null");
        }
        Set<Object> keys = targetDataSources.keySet();
        int salveNum = keys.size() - 1;
        List<String> updatedSlaveDataSourceKeys = new ArrayList<>(Math.max(salveNum, 0));
        for (Object key : keys) {
            if (!this.masterKey.equals(key.toString())) {
                updatedSlaveDataSourceKeys.add(key.toString());
            }
        }
        synchronized (this) {
            this.slaveDataSourceKeys = updatedSlaveDataSourceKeys;
            this.keyIndex = 0;
        }
    }

    /**
     * 设置数据源映射，并重新初始化从库 key 列表。
     *
     * @param targetDataSources 数据源名称与数据源的映射
     */
    @Override
    public void setTargetDataSources(@NonNull Map<Object, Object> targetDataSources) {
        Assert.notEmpty(targetDataSources, "targetDataSources can not be null");
        synchronized (this) {
            super.setTargetDataSources(targetDataSources);
            this.initSlaveDataSourceKeys(targetDataSources);
        }
    }

    /**
     * 获取当前线程使用的数据源查找 key。
     *
     * @return 当前线程使用的数据源 key
     */
    @Override
    protected Object determineCurrentLookupKey() {
        return this.getDataSourceKey();
    }

    /**
     * 标记当前线程使用主库。
     */
    public void markMaster() {
        this.CURRENT_DATASOURCE_KEY_TL.set(this.masterKey);
        if (log.isDebugEnabled()) {
            log.debug("switch master datasource");
        }
    }

    /**
     * 按轮询顺序标记当前线程使用从库。
     * 没有配置从库时，回退使用主库。
     */
    public void markSlave() {
        synchronized (this) {
            List<String> slaveKeys = this.slaveDataSourceKeys;
            if (slaveKeys == null || slaveKeys.isEmpty()) {
                this.markMaster();
                return;
            }
            if (this.keyIndex >= slaveKeys.size()) {
                this.keyIndex = 0;
            }
            this.CURRENT_DATASOURCE_KEY_TL.set(slaveKeys.get(this.keyIndex));
            this.keyIndex++;
            if (this.keyIndex >= slaveKeys.size()) {
                this.keyIndex = 0;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("switch {} datasource", this.CURRENT_DATASOURCE_KEY_TL.get());
        }
    }

    /**
     * 根据完整数据源 key 标记当前线程使用的数据源。
     * key 不存在或对应从库未配置时，回退使用主库。
     *
     * @param key 数据源 key
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

    /**
     * 根据包含指定字符串的数据源 key 标记当前线程使用的数据源。
     * 如果存在多个匹配项，则使用按配置顺序找到的第一个 key；没有匹配项时切换到主数据源。
     *
     * @param keyPart 数据源 key 中需要包含的字符串
     */
    public void markByKeyContains(String keyPart) {
        if (keyPart == null || keyPart.isEmpty()) {
            throw new IllegalArgumentException("keyPart can not be null");
        }
        if (this.slaveDataSourceKeys != null) {
            for (String key : this.slaveDataSourceKeys) {
                if (key.contains(keyPart)) {
                    this.markByKey(key);
                    return;
                }
            }
        }
        this.markMaster();
    }

    /**
     * 根据完整数据源 key 获取数据源。
     * 如果 key 不存在，则返回主数据源。
     *
     * @param key 数据源 key
     * @return 对应的数据源，未找到时返回主数据源
     */
    public DataSource getDataSourceByKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key can not be null");
        }
        DataSource dataSource = this.getResolvedDataSources().get(key);
        if (dataSource == null) {
            dataSource = this.getResolvedDataSources().get(this.masterKey);
        }
        return dataSource;
    }

    /**
     * 根据数据源 key 中包含的指定字符串获取数据源。
     * 如果存在多个匹配项，则使用按配置顺序找到的第一个 key；没有匹配项时返回主数据源。
     *
     * @param keyPart 数据源 key 中需要包含的字符串
     * @return 对应的数据源，未找到时返回主数据源
     */
    public DataSource getDataSourceByKeyContains(String keyPart) {
        if (keyPart == null || keyPart.isEmpty()) {
            throw new IllegalArgumentException("keyPart can not be null");
        }
        if (this.slaveDataSourceKeys != null) {
            for (String key : this.slaveDataSourceKeys) {
                if (key.contains(keyPart)) {
                    return this.getDataSourceByKey(key);
                }
            }
        }
        return this.getDataSourceByKey(this.masterKey);
    }

    /**
     * 获取当前线程使用的数据源 key。
     * 当前线程尚未标记数据源时，默认使用主数据源。
     *
     * @return 当前线程使用的数据源 key
     */
    public String getDataSourceKey() {
        String key = this.CURRENT_DATASOURCE_KEY_TL.get();
        if (key == null) {
            key = this.masterKey;
            this.markMaster();
        }
        return key;
    }

    /**
     * 处理数据源切换事件，并根据事件类型切换当前线程的数据源。
     * {@link SwitchDataSourceContainsEvent} 使用包含匹配，其它事件使用完整 key 匹配。
     *
     * @param event 数据源切换事件
     */
    @Override
    public void onApplicationEvent(@NonNull SwitchDataSourceEvent event) {
        if (event instanceof SwitchDataSourceContainsEvent) {
            this.markByKeyContains(event.getSource());
        } else {
            this.markByKey(event.getSource());
        }
    }
}
