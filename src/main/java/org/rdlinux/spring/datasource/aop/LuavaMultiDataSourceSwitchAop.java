package org.rdlinux.spring.datasource.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.rdlinux.spring.datasource.LuavaMultiDataSource;
import org.rdlinux.spring.datasource.annotation.DataSourceSwitch;

import javax.annotation.PostConstruct;

@Aspect
public class LuavaMultiDataSourceSwitchAop {
    private LuavaMultiDataSource multiDataSource;

    public LuavaMultiDataSourceSwitchAop(LuavaMultiDataSource multiDataSource) {
        this.setMultiDataSource(multiDataSource);
    }

    @Pointcut("@annotation(org.rdlinux.spring.datasource.annotation.DataSourceSwitch)")
    public void pointcut() {
    }

    @Around("pointcut()")
    public Object switchDataSource(ProceedingJoinPoint joinPoint) throws Throwable {
        if (this.multiDataSource == null) {
            throw new IllegalArgumentException("multiDataSource can not be null");
        }
        String oldKey = this.multiDataSource.getDataSourceKey();
        DataSourceSwitch dataSourceSwitch = ((MethodSignature) joinPoint.getSignature()).getMethod()
                .getAnnotation(DataSourceSwitch.class);
        String dataSourceKey = dataSourceSwitch.value().trim();
        if (!dataSourceKey.isEmpty()) {
            this.multiDataSource.markByKey(dataSourceKey);
        } else if (dataSourceSwitch.slave()) {
            this.multiDataSource.markSlave();
        } else {
            this.multiDataSource.markMaster();
        }
        Object result;
        try {
            result = joinPoint.proceed();
        } finally {
            this.multiDataSource.markByKey(oldKey);
        }
        return result;
    }

    public void setMultiDataSource(LuavaMultiDataSource multiDataSource) {
        if (multiDataSource == null) {
            throw new IllegalArgumentException("multiDataSource can not be null");
        }
        this.multiDataSource = multiDataSource;
    }

    @PostConstruct
    public void init() {
        if (this.multiDataSource == null) {
            throw new IllegalArgumentException("multiDataSource can not be null");
        }
    }
}
