# luava-spring-multidatasource
spring多数据源
# 如何使用
## 1. maven引入
```
<dependency>
  <groupId>org.linuxprobe</groupId>
  <artifactId>luava-spring-multidatasource</artifactId>
  <version>0.0.2.RELEASE</version>
</dependency>
```
## 2. yml配置
```
spring:
  datasources:
    # master库必须有,下面的从库可以没有
    master:
      jdbc-url: jdbc:mysql://127.0.0.1:3306/uni?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: root
      password: 123456
      driverClassName: com.mysql.cj.jdbc.Driver
    # 从库; 从库可以随便起名, 但不能是master
    slave:
      jdbc-url: jdbc:mysql://192.168.160.2:3306/uni?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: root
      password: 123456
      driverClassName: com.mysql.cj.jdbc.Driver
    # 从库1
    slave1:
      jdbc-url: jdbc:mysql://192.168.160.2:3306/uni?useUnicode=true&characterEncoding=UTF-8&useSSL=false
      username: root
      password: 123456
      driverClassName: com.mysql.cj.jdbc.Driver
```
## 3. spring boot java bean配置(spring + spring mvc 也可自行转换为xml配置)
```
import java.util.Map;

import MultiDataSource;
import DataSourceSwitch;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;
import lombok.Setter;

@Configuration
@ConfigurationProperties(prefix = "spring")
@Getter
@Setter
public class DataSourceConfiguration {
	private Map<String, HikariDataSource> datasources;

        // 配置数据源
	@SuppressWarnings("unchecked")
	@Bean
	public MultiDataSource multiDataSource() {
		return new MultiDataSource((Map<Object, Object>) ((Map<?, ?>) datasources));
	}

        // 配置切面
	@Bean
	public DataSourceAop dataSourceAop(MultiDataSource multiDataSource) {
		return new DataSourceAop(multiDataSource);
	}
        // 配置事务管理
	@Bean
	DataSourceTransactionManager transactionManager(MultiDataSource multiDataSource) {
		return new DataSourceTransactionManager(multiDataSource);
	}
}
```
## 4. contoller或service的方法注解(DataSorce)切换数据源
1. 当mater为true时, 标记使用主库, 这是默认的;
2. 当slave为true时, 标记使用从库, 此时mater的值将失效, 如何有多个从库, 将轮询使用每个从库来达到负载均衡;
3. 当value的值不为空时, slave和master的值将失效, 切换到用户指定的数据源.

```
package org.linuxprobe.universalcrudspringbootdemo.controller;

import org.linuxprobe.crud.core.query.Page;
import org.linuxprobe.universalcrudspringbootdemo.model.Permission;
import org.linuxprobe.universalcrudspringbootdemo.query.PermissionQuery;
import org.linuxprobe.universalcrudspringbootdemo.service.PermissonService;
import DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/permission")
public class PermissionController {
	@Autowired
	private PermissonService service;
        // 标记从库,会启用负载均衡
	@GetMapping("/getPageInfo")
	@DataSourceSwitch(slave = true)
	public Page<Permission> getPageInfo(PermissionQuery param) {
		return this.service.getPageInfo(param);
	}

        // 标记主库
	@GetMapping("/getPageInfo2")
	@DataSourceSwitch
	public Page<Permission> getPageInfo2(PermissionQuery param) {
		return this.service.getPageInfo(param);
	}
	
	// 标记指定库
	@GetMapping("/getPageInfo3")
	@DataSourceSwitch(value = "slave1")
	public Page<Permission> getPageInfo3(PermissionQuery param) {
		return this.service.getPageInfo(param);
	}
}

```
