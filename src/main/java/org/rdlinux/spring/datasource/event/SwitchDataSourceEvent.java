package org.rdlinux.spring.datasource.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * 根据完整数据源 key 切换数据源的事件。
 */
public class SwitchDataSourceEvent extends ApplicationEvent {
    /**
     * 序列化版本号。
     */
    private static final long serialVersionUID = 2930212687443070167L;

    /**
     * 创建根据完整 key 切换数据源的事件。
     *
     * @param key 数据源 key
     */
    public SwitchDataSourceEvent(String key) {
        super(key);
        Assert.hasLength(key, "datasource key can not be empty");
    }

    /**
     * 获取数据源 key。
     *
     * @return 数据源 key
     */
    @Override
    public String getSource() {
        return (String) super.getSource();
    }
}
