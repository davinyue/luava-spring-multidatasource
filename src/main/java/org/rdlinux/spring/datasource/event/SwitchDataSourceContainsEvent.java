package org.rdlinux.spring.datasource.event;

/**
 * 根据数据源 key 中包含的指定字符串切换数据源的事件。
 */
public class SwitchDataSourceContainsEvent extends SwitchDataSourceEvent {
    /**
     * 序列化版本号。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 创建根据包含字符串切换数据源的事件。
     *
     * @param keyPart 数据源 key 中需要包含的字符串
     */
    public SwitchDataSourceContainsEvent(String keyPart) {
        super(keyPart);
    }
}
