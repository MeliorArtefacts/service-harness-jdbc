# Melior Service Harness :: JDBC
<div style="display: inline-block;">
<img src="https://img.shields.io/badge/version-2.2-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/production-ready-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/compatibility-spring_boot_2.4.5-green?style=for-the-badge"/>
</div>
<div style="display: inline-block;">
<img src="https://img.shields.io/badge/version-2.3-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/production-ready-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/compatibility-spring_boot_2.4.5-green?style=for-the-badge"/>
</div>

## Artefact
Get the artefact and the POM file in the *artefact* folder.
```
<dependency>
    <groupId>org.melior</groupId>
    <artifactId>melior-harness-jdbc</artifactId>
    <version>2.3</version>
</dependency>
```

## Connection Pool
The Melior JDBC connection pool replaces the HikariCP JDBC connection pool that comes with Spring Boot by default.  The property defaults used by HikariCP catch many new developers out, with poor performance under load and with data integrity problems in multi-statement transactions.  The Melior JDBC connection pool offers better property defaults that avoid these problems.  The Melior JDBC connection pool is more resilient against sudden, large increases in load than HikariCP is.

Create a bean to instantiate the JDBC connection pool.
```
@Bean("mydb")
@ConfigurationProperties("mydb")
public DataSource dataSource() {
    return DataSourceBuilder.create().type(DataSource.class).build();
}
```

The JDBC connection pool is auto-configured from the application properties.
```
mydb.url=jdbc:mysql://mydbhost:3306/mydb
mydb.username=user
mydb.password=password
mydb.request-timeout=30
mydb.inactivity-timeout=3600
```

Wire in and use the JDBC connection pool.
```
@Autowired
private DataSource dataSource;

public void foo() throws DataAccessException {
    Connection connection;

    try {
        connection = dataSource.getConnection();
    }
    catch (Exception ex) {
        throw new DataAccessException(ex);
    }

    try {
        // do JDBC calls
    }
    catch (Exception ex) {
        connection.rollback();
        throw new DataAccessException(ex);
    }
    finally {
        connection.close();
    }
}
```

&nbsp;  
The JDBC connection pool may be configured using these application properties.

|Name|Default|Description|
|:--------------------|:---|:---|
|`driver-class-name`||The class name to use to instantiate the JDBC driver; ony needed if the JDBC driver cannot be instantiate from the URL alone|
|`url`||The URL of the target database|
|`username`||The user name required by the target database|
|`password`||The password required by the target database|
|`catalog`||The default catalog to use when accessing the target database|
|`schema`||The default schema to use when accessing the target database|
|`read-only`|false|Indicates if the target database should be treated as a read-only database|
|`transaction-isolation`||The transaction isolation level; defaults to what the JDBC driver normally sets|
|`auto-commit`|false|Indicates if individual JDBC statements must automatically be committed after being executed in the target database|
|`minimum-connections`|0|The minimum number of connections to open to the target database|
|`maximum-connections`|unlimited|The maximum number of connections to open to the target database|
|`connection-timeout`|30 s|The amount of time to allow for a new connection to open to the target database|
|`validate-on-borrow`|false|Indicates if a connection must be validated when it is borrowed from the JDBC connection pool|
|`validation-timeout`|5 s|The amount of time to allow for a connection to be validated|
|`request-timeout`|60 s|The amount of time to allow for a request to the target database to complete|
|`backoff-period`|1 s|The amount of time to back off when the circuit breaker trips|
|`backoff-multiplier`|1|The factor with which to increase the backoff period when the circuit breaker trips repeatedly|
|`backoff-limit`||The maximum amount of time to back off when the circuit breaker trips repeatedly|
|`inactivity-timeout`|300 s|The amount of time to allow before surplus connections to the target database are pruned|
|`maximum-lifetime`|unlimited|The maximum lifetime of a connection to the target database|
|`prune-interval`|60 s|The interval at which surplus connections to the target database are pruned|
|`statement-cache-size`|100|The maximum number of JDBC statements to retain in the statement cache|
|`log-arguments`|false|Indicates if the SQL text and individual parameters of the JDBC statements must be logged in the detailed trace logs|

&nbsp;  
## Data Access Object
Use the data access object harness to get the JDBC connection pool and all the JDBC helpers, along with the standard Melior logging system and a configuration object that may be used to access the application properties anywhere and at any time in the application code, even in the constructor.
```
@Component
public class MyDBAccess extends DataAccessObject

public MyDBAccess(ServiceContext serviceContext, @Qualifier("mydb") DataSource dataSource) throws ApplicationException {
    super(serviceContext, dataSource);
}
```

Implement a **configure** method to have more control over accessing the application properties than using @Value annotations or using constructor injection.
```
protected void configure() throws ApplicationException {
    tableName = configuration.getProperty("mydb.table-name");
    rowLimit = configuration.getProperty("mydb.row-limit", int.class);
}
```

The data access object harness provides many helpers.  Here are some examples.
```
// automatic NULL handling when setting on statement
setLong(statement, 1, myBoxedLong);

// automatic NULL handling when getting from result set
Long myLong = getBoxedLong(resultSet, 1);

// parsing of timestamp or date
statement.setTimestamp(1, parseTimestamp("2022-05-26 17:38:21"));

// conversion to string with default value when NULL
String myString = getFloatAsString(statement, 1, "no-value");

// adjustment of timestamp or date
statement.setTimestamp(1, adjustTimestamp(myTimestamp, Duration.ofHours(-8)));
```

## References
Refer to the [**Melior Service Harness :: Core**](https://github.com/MeliorArtefacts/service-harness-core) module for detail on the Melior logging system and available utilities.
