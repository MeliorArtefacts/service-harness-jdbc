/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.jdbc.datasource;
import java.util.Properties;
import org.melior.jdbc.core.TransactionIsolation;
import org.melior.jdbc.util.TimeDelta;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.util.number.Clamp;

/**
 * Configuration parameters for a {@code DataSource}, with defaults.
 * @author Melior
 * @since 2.2
 */
public class DataSourceConfig {

    private String driverClassName;

    private String url;

    private String username;

    private String password;

    private String catalog;

    private String schema;

    private boolean readOnly = false;

    private TransactionIsolation transactionIsolation = null;

    private boolean autoCommit = false;

    private int minimumConnections = 0;

    private int maximumConnections = Integer.MAX_VALUE;

    private int connectionTimeout = 30;

    private boolean validateOnBorrow = false;

    private int validationTimeout = 5;

    private int requestTimeout = 60;

    private int backoffPeriod = 1 * 1000;

    private float backoffMultiplier = 1;

    private int backoffLimit = 0 * 1000;

    private int inactivityTimeout = 300 * 1000;

    private int maximumLifetime = 0 * 1000;

    private int pruneInterval = 60 * 1000;

    private boolean cacheMetadata = false;

    private int statementCacheSize = 100;

    private boolean logArguments = false;

    private Properties connectionProperties;

    private TimeDelta timeDelta;

    /**
     * Constructor.
     */
    public DataSourceConfig() {

        super();

        connectionProperties = new Properties();

        timeDelta = new TimeDelta();
    }

    /**
     * Get driver class name.
     * @return The driver class name
     */
    public String getDriverClassName() {
        return driverClassName;
    }

    /**
     * Set driver class name.
     * @param driverClassName The driver class name
     */
    public void setDriverClassName(
        final String driverClassName) {
        this.driverClassName = driverClassName;
    }

    /**
     * Get URL.
     * @return The URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Set URL.
     * @param url The URL
     */
    public void setUrl(
        final String url) {
        this.url = url;
    }

    /**
     * Set URL.
     * @param url The URL
     */
    public void setJdbcUrl(
        final String url) {
        setUrl(url);
    }

    /**
     * Get user name.
     * @return The user name
     */
    public String getUsername() {
        return username;
    }

    /**
     * Set user name.
     * @param username The user name
     */
    public void setUsername(
        final String username) {
        this.username = username;

        if (username != null) {
            connectionProperties.setProperty("user", username);
        }
        else {
            connectionProperties.remove("user");
        }

    }

    /**
     * Get password.
     * @return The password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set password.
     * @param password The password
     */
    public void setPassword(
        final String password) {
        this.password = password;

        if (password != null) {
            connectionProperties.setProperty("password", password);
        }
        else {
            connectionProperties.remove("password");
        }

    }

    /**
     * Get catalog.
     * @return The catalog
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Set catalog.
     * @param catalog The catalog
     */
    public void setCatalog(
        final String catalog) {
        this.catalog = catalog;
    }

    /**
     * Get schema.
     * @return The schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set schema.
     * @param schema The schema
     */
    public void setSchema(
        final String schema) {
        this.schema = schema;
    }

    /**
     * Get read only indicator.
     * @return The read only indicator
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Set read only indicator.
     * @param readOnly The read only indicator
     */
    public void setReadOnly(
        final boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Get transaction isolation.
     * @return The transaction isolation
     */
    public TransactionIsolation getTransactionIsolation() {
        return transactionIsolation;
    }

    /**
     * Set transaction isolation.
     * @param transactionIsolation The transaction isolation
     */
    public void setTransactionIsolation(
        final TransactionIsolation transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    /**
     * Get auto commit indicator.
     * @return The auto commit indicator
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Set auto commit indicator.
     * @param autoCommit The auto commit indicator
     */
    public void setAutoCommit(
        final boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Get minimum number of connections.
     * @return The minimum number of connections
     */
    public int getMinimumConnections() {
        return minimumConnections;
    }

    /**
     * Set minimum number of connections.
     * @param minimumConnections The minimum number of connections
     * @throws ApplicationException if the minimum number of connections is invalid
     */
    public void setMinimumConnections(
        final int minimumConnections) throws ApplicationException {

        if ((this.maximumConnections > 0)
            && (minimumConnections > this.maximumConnections)) {
            throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Minimum number of connections may not be more than maximum.");
        }

        this.minimumConnections = Clamp.clampInt(minimumConnections, 0, Integer.MAX_VALUE);
    }

    /**
     * Get maximum number of connections.
     * @return The maximum number of connections
     */
    public int getMaximumConnections() {
        return maximumConnections;
    }

    /**
     * Set maximum number of connections.
     * @param maximumConnections The maximum number of connections
     * @throws ApplicationException if the maximum number of connections is invalid
     */
    public void setMaximumConnections(
        final int maximumConnections) throws ApplicationException {

        if ((maximumConnections > 0)
            && (maximumConnections < this.minimumConnections)) {
            throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Maximum number of connections may not be less than minimum.");
        }

        this.maximumConnections = Clamp.clampInt(maximumConnections, 0, Integer.MAX_VALUE);
    }

    /**
     * Get connection timeout.
     * @return The connection timeout
     */
    public int getConnectionTimeout() {
        return (connectionTimeout == 0) ? getRequestTimeout() : connectionTimeout;
    }

    /**
     * Set connection timeout.
     * @param connectionTimeout The connection timeout, specified in seconds
     */
    public void setConnectionTimeout(
        final int connectionTimeout) {
        this.connectionTimeout = Clamp.clampInt(connectionTimeout, 0, Integer.MAX_VALUE);
    }

    /**
     * Get login timeout.
     * @return The login timeout
     */
    public int getLoginTimeout() {
        return getConnectionTimeout();
    }

    /**
     * Set login timeout.
     * @param loginTimeout The login timeout
     */
    public void setLoginTimeout(
        final int loginTimeout) {
        setConnectionTimeout(loginTimeout);
    }

    /**
     * Get validate on borrow indicator.
     * @return The validate on borrow indicator
     */
    public boolean isValidateOnBorrow() {
        return validateOnBorrow;
    }

    /**
     * Set validate on borrow indicator.
     * @param validateOnBorrow The validate on borrow indicator
     */
    public void setValidateOnBorrow(
        final boolean validateOnBorrow) {
        this.validateOnBorrow = validateOnBorrow;
    }

    /**
     * Get validation timeout.
     * @return The validation timeout
     */
    public int getValidationTimeout() {
        return (validationTimeout == 0) ? getConnectionTimeout() : validationTimeout;
    }

    /**
     * Set validation timeout.
     * @param validationTimeout The validation timeout, specified in seconds
     */
    public void setValidationTimeout(
        final int validationTimeout) {
        this.validationTimeout = Clamp.clampInt(validationTimeout, 0, Integer.MAX_VALUE);
    }

    /**
     * Get request timeout.
     * @return The request timeout
     */
    public final int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Set request timeout.
     * @param requestTimeout The request timeout, specified in seconds
     */
    public final void setRequestTimeout(
        final int requestTimeout) {
        this.requestTimeout = Clamp.clampInt(requestTimeout, 0, Integer.MAX_VALUE);
    }

    /**
     * Get backoff period.
     * @return The backoff period
     */
    public final int getBackoffPeriod() {
        return backoffPeriod;
    }

    /**
     * Set backoff period.
     * @param backoffPeriod The backoff period, specified in seconds
     */
    public final void setBackoffPeriod(
        final int backoffPeriod) {
        this.backoffPeriod = Clamp.clampInt(backoffPeriod * 1000, 1000, Integer.MAX_VALUE);
    }

    /**
     * Get backoff multiplier.
     * @return The backoff multiplier
     */
    public float getBackoffMultiplier() {
        return backoffMultiplier;
    }

    /**
     * Set backoff multiplier.
     * @param backoffMultiplier The backoff multiplier
     */
    public void setBackoffMultiplier(
        final float backoffMultiplier) {
        this.backoffMultiplier = Clamp.clampFloat(backoffMultiplier, 0, Float.MAX_VALUE);
    }

    /**
     * Get backoff limit.
     * @return The backoff limit
     */
    public int getBackoffLimit() {
        return backoffLimit;
    }

    /**
     * Set backoff limit.
     * @param backoffLimit The backoff limit, specified in seconds
     */
    public void setBackoffLimit(
        final int backoffLimit) {
        this.backoffLimit = Clamp.clampInt(backoffLimit * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get connection inactivity timeout.
     * @return The connection inactivity timeout
     */
    public final int getInactivityTimeout() {
        return inactivityTimeout;
    }

    /**
     * Set connection inactivity timeout
     * @param inactivityTimeout The connection inactivity timeout, specified in seconds
     */
    public final void setInactivityTimeout(
        final int inactivityTimeout) {
        this.inactivityTimeout = Clamp.clampInt(inactivityTimeout * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get maximum connection lifetime.
     * @return The maximum connection lifetime
     */
    public int getMaximumLifetime() {
        return maximumLifetime;
    }

    /**
     * Set maximum connection lifetime.
     * @param maximumLifetime The maximum connection lifetime, specified in seconds
     */
    public void setMaximumLifetime(
        final int maximumLifetime) {
        this.maximumLifetime = Clamp.clampInt(maximumLifetime * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get connection prune interval.
     * @return The connection prune interval
     */
    public int getPruneInterval() {
        return pruneInterval;
    }

    /**
     * Set connection prune interval.
     * @param pruneInterval The connection prune interval, specified in seconds
     */
    public void setPruneInterval(
        final int pruneInterval) {
        this.pruneInterval = Clamp.clampInt(pruneInterval * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get cache metadata indicator.
     * @return The cache metadata indicator
     */
    public boolean isCacheMetadata() {
        return cacheMetadata;
    }

    /**
     * Set cache metadata indicator.
     * @param cacheMetadata The cache metadata indicator
     */
    public void setCacheMetadata(
        final boolean cacheMetadata) {
        this.cacheMetadata = cacheMetadata;
    }

    /**
     * Get statement cache size.
     * @return The statement cache size
     */
    public final int getStatementCacheSize() {
        return statementCacheSize;
    }

    /**
     * Set statement cache size.
     * @param statementCacheSize The statement cache size
     */
    public final void setStatementCacheSize(
        final int statementCacheSize) {
        this.statementCacheSize = Clamp.clampInt(statementCacheSize, 0, Integer.MAX_VALUE);
    }

    /**
     * Get log arguments indicator.
     * @return The log arguments indicator
     */
    public boolean isLogArguments() {
        return logArguments;
    }

    /**
     * Set log arguments indicator.
     * @param logArguments The log arguments indicator
     */
    public void setLogArguments(
        final boolean logArguments) {
        this.logArguments = logArguments;
    }

    /**
     * Get connection properties.  The connection properties consist of the username and password,
     * and any other connection properties derived from the data source.
     * @return The connection properties
     */
    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    /**
     * Get time delta.
     * @return The time delta
     */
    public TimeDelta getTimeDelta() {
        return timeDelta;
    }

}
