# luava-spring-multidatasource

基于 Spring `AbstractRoutingDataSource` 的多数据源切换组件，支持主库、从库轮询、精确 key 切换、包含 key 切换、注解切换和事件切换。

## 功能概览

- 默认使用主数据源，主数据源 key 默认为 `master`。
- 通过 `markSlave()` 在多个从库之间按顺序轮询。
- 通过完整 key 切换数据源。
- 通过 key 包含的字符串切换数据源。
- 通过 `@DataSourceSwitch` 注解切换数据源。
- 通过 `SwitchDataSourceEvent` 和 `SwitchDataSourceContainsEvent` 发布切换事件。
- 根据完整 key 或包含字符串获取 `DataSource` 实例。

## 1. 添加依赖

### 组件依赖

```xml
<dependency>
    <groupId>org.rdlinux</groupId>
    <artifactId>luava-spring-multidatasource</artifactId>
    <version>0.0.5.RELEASE</version>
</dependency>
```

组件中的 Spring 依赖使用 `provided` 作用域，应用还需要自行提供 Spring JDBC、Spring Context、数据库驱动和连接池依赖。

### HikariCP

Spring Boot 项目可以使用 `spring-boot-starter-jdbc`，它默认包含 HikariCP；非 Spring Boot 项目可以直接引入 HikariCP：

```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>${hikari.version}</version>
</dependency>
```

### Druid

```xml
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>druid</artifactId>
    <version>${druid.version}</version>
</dependency>
```

数据库驱动以 MySQL 为例：

```xml
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <scope>runtime</scope>
</dependency>
```

`${hikari.version}`、`${druid.version}` 和数据库驱动版本由应用自身的 Maven `parent` 或 `dependencyManagement` 管理。

如果使用注解切换，还需要启用 Spring AOP。Spring Boot 项目可以引入：

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
```

## 2. 数据源配置

配置前需要注意：

- 配置前缀使用 `spring.datasources`，不是 Spring Boot 默认的单数据源 `spring.datasource`。
- `master` 必须存在，或者使用构造方法指定自定义主库 key。
- 除主库外的其它 key 都会被识别为从库。
- Hikari 使用 `jdbc-url`；Druid 使用 `url`。

### 2.1 Hikari 配置

```yaml
spring:
  datasources:
    master:
      jdbc-url: jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      maximum-pool-size: 10
    read-replica-1:
      jdbc-url: jdbc:mysql://127.0.0.1:3307/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      maximum-pool-size: 10
    read-replica-2:
      jdbc-url: jdbc:mysql://127.0.0.1:3308/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      maximum-pool-size: 10
```

对应的 Java 配置如下：

```java
package com.example.config;

import com.zaxxer.hikari.HikariDataSource;
import org.rdlinux.spring.datasource.LuavaMultiDataSource;
import org.rdlinux.spring.datasource.aop.MultiDataSourceSwitchAop;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "spring")
@EnableAspectJAutoProxy
public class DataSourceConfiguration {
    private Map<String, HikariDataSource> datasources;

    public void setDatasources(Map<String, HikariDataSource> datasources) {
        this.datasources = datasources;
    }

    @Bean
    public LuavaMultiDataSource multiDataSource() {
        Map<String, DataSource> targetDataSources = new LinkedHashMap<>();
        targetDataSources.putAll(this.datasources);
        return new LuavaMultiDataSource(targetDataSources);
    }

    @Bean
    public MultiDataSourceSwitchAop dataSourceAop(LuavaMultiDataSource multiDataSource) {
        return new MultiDataSourceSwitchAop(multiDataSource);
    }

    @Bean
    public DataSourceTransactionManager transactionManager(LuavaMultiDataSource multiDataSource) {
        return new DataSourceTransactionManager(multiDataSource);
    }
}
```

### 2.2 Druid 配置

```yaml
spring:
  datasources:
    master:
      url: jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      initial-size: 1
      min-idle: 1
      max-active: 10
    read-replica-1:
      url: jdbc:mysql://127.0.0.1:3307/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      initial-size: 1
      min-idle: 1
      max-active: 10
    read-replica-2:
      url: jdbc:mysql://127.0.0.1:3308/app?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: app
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
      initial-size: 1
      min-idle: 1
      max-active: 10
```

Druid 的 Java 配置与 Hikari 基本相同，只需要将连接池类型替换为 `DruidDataSource`：

```java
import com.alibaba.druid.pool.DruidDataSource;

// 其它配置与 Hikari 示例相同
private Map<String, DruidDataSource> datasources;

public void setDatasources(Map<String, DruidDataSource> datasources) {
    this.datasources = datasources;
}

@Bean
public LuavaMultiDataSource multiDataSource() {
    Map<String, DataSource> targetDataSources = new LinkedHashMap<>();
    targetDataSources.putAll(this.datasources);
    return new LuavaMultiDataSource(targetDataSources);
}
```

Hikari 和 Druid 二选一，不要在同一个配置类中同时声明两种类型的 `datasources` 字段。

## 3. 使用注解切换

`@DataSourceSwitch` 只能通过完整 key 指定数据源：

```java
import org.rdlinux.spring.datasource.annotation.DataSourceSwitch;

public class UserService {
    /** 使用主库。 */
    @DataSourceSwitch
    public void write() {
    }

    /** 在所有从库之间轮询。 */
    @DataSourceSwitch(slave = true)
    public void queryWithLoadBalance() {
    }

    /** 切换到指定从库。 */
    @DataSourceSwitch(value = "read-replica-1")
    public void queryFromReplica() {
    }
}
```

注解优先级如下：

1. `value` 不为空时，按完整 key 切换。
2. `value` 为空且 `slave = true` 时，按轮询顺序切换从库。
3. `value` 和 `slave` 都未生效时，切换主库。

## 4. 通过 Java 方法切换或获取数据源

```java
import javax.sql.DataSource;

// 切换当前线程的数据源
multiDataSource.markMaster();
multiDataSource.markSlave();
multiDataSource.markByKey("read-replica-1");
multiDataSource.markByKeyContains("read-replica");

// 获取数据源实例，不改变当前线程的数据源标记
DataSource dataSource1 = multiDataSource.getDataSourceByKey("read-replica-1");
DataSource dataSource2 = multiDataSource.getDataSourceByKeyContains("read-replica");
```

`markByKeyContains` 和 `getDataSourceByKeyContains` 会按从库 key 的配置顺序使用第一个匹配项；没有匹配项时使用主库。`getDataSourceByKey` 传入不存在的 key 时也会返回主库。

数据源切换状态保存在当前线程中。手动切换后执行数据库操作时，应在 `finally` 中恢复原来的 key；使用 `@DataSourceSwitch` 时，AOP 会自动恢复。

## 5. 通过事件切换数据源

将事件发布到 Spring 容器即可切换当前线程的数据源：

```java
import org.rdlinux.spring.datasource.event.SwitchDataSourceContainsEvent;
import org.rdlinux.spring.datasource.event.SwitchDataSourceEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class DataSourceSwitchService {
    private final ApplicationEventPublisher eventPublisher;

    public DataSourceSwitchService(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void switchByExactKey() {
        this.eventPublisher.publishEvent(new SwitchDataSourceEvent("read-replica-1"));
    }

    public void switchByKeyPart() {
        this.eventPublisher.publishEvent(new SwitchDataSourceContainsEvent("read-replica"));
    }
}
```

事件区别：

- `SwitchDataSourceEvent`：按完整 key 匹配。
- `SwitchDataSourceContainsEvent`：按 key 包含的字符串匹配。
- 参数为 `null` 或空字符串时会抛出参数异常。

## 6. 自定义主库 key

如果主库 key 不是 `master`，使用带 `masterKey` 的构造方法，并保证映射中存在该 key：

```java
Map<String, DataSource> targetDataSources = new LinkedHashMap<>();
targetDataSources.put("primary", primaryDataSource);
targetDataSources.put("read-replica-1", replicaDataSource);

LuavaMultiDataSource multiDataSource =
        new LuavaMultiDataSource("primary", targetDataSources);
```

## 7. 注意事项

- 主库数据源必须配置，否则初始化时会失败。
- 所有非主库 key 都会被作为从库参与轮询和包含匹配。
- 包含匹配区分大小写。
- Spring 管理的 `LuavaMultiDataSource` 会自动完成路由数据源初始化；手动创建实例后再调用获取数据源的方法前，应先完成 Spring 数据源初始化流程。
