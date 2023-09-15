package org.rdlinux.spring.datasource.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * 数据源切换事件
 */
public class SwitchDataSourceEvent extends ApplicationEvent {
    private static final long serialVersionUID = 2930212687443070167L;

    public SwitchDataSourceEvent(String key) {
        super(key);
        Assert.hasLength(key, "datasource key can not be empty");
    }

    @Override
    public String getSource() {
        return (String) super.getSource();
    }
}
