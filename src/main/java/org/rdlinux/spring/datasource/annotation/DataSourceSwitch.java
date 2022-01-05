package org.rdlinux.spring.datasource.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 标记使用数据源
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface DataSourceSwitch {
    /**
     * 是否标记主库
     */
    boolean master() default true;

    /**
     * 是否标记从库, 当为true时, master失效
     */
    boolean slave() default false;

    /**
     * 指定数据源名称, 当不为空时, slave和master失效
     */
    String value() default "";
}
