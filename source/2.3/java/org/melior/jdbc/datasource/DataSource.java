/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.datasource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.melior.jdbc.core.SQLState;
import org.melior.jdbc.enhancer.StatementEnhancer;
import org.melior.jdbc.pool.ConnectionPool;
import org.melior.jdbc.session.SessionController;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.exception.ApplicationException;

/**
 * Implements a factory for JDBC {@code Connection} objects, for connections to
 * the physical data source that this {@code DataSource} object represents. The
 * data source writes statistics from the underlying connection pool to the logs
 * whenever a {@code Connection} is borrowed from the pool.
 * @author Melior
 * @since 2.2
 */
public class DataSource extends DataSourceConfig implements javax.sql.DataSource{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    private SessionController sessionController;

    private StatementEnhancer statementEnhancer;

    private ConnectionPool connectionPool;

  /**
   * Constructor.
   */
  public DataSource(){
        super();
  }

  /**
   * Initialize data source.
   * @throws SQLException when unable to initialize the data source
   */
  public void initialize() throws SQLException{

        if (connectionPool != null){
      return;
    }

    try{
            connectionPool = new ConnectionPool(this);
    }
    catch (Exception exception){
      throw new SQLException("Failed to create connection pool: " + exception.getMessage(), SQLState.CONNECTION_FAILURE.value(), exception);
    }

  }

  /**
   * Set driver class name.
   * @param driverClassName The driver class name
   */
  public void setDriverClassName(
    final String driverClassName){
        super.setDriverClassName(driverClassName);

        registerDriver(driverClassName);
  }

  /**
   * Set minimum number of connections.
   * @param minimumConnections The minimum number of connections
   * @throws ApplicationException when the minimum number of connections is invalid
   */
  public void setMinimumConnections(
    final int minimumConnections) throws ApplicationException{
        super.setMinimumConnections(minimumConnections);

        if (connectionPool != null){
            connectionPool.resizePool();
    }

  }

  /**
   * Get session controller.
   * @return The session controller
   */
  public SessionController getSessionController(){
    return sessionController;
  }

  /**
   * Get statement enhancer.
   * @return The statement enhancer
   */
  public StatementEnhancer getStatementEnhancer(){
    return statementEnhancer;
  }

  /**
   * Get connection to database.
   * @return The connection
   * @throws SQLException when unable to get a connection
   */
  public Connection getConnection() throws SQLException{
        String methodName = "getConnection";
    Connection connection;

        initialize();

    logger.debug(methodName, "Connection pool [", connectionPool.getPoolId(), "]: total=", connectionPool.getTotalConnections(),
      ", active=", connectionPool.getActiveConnections(), ", deficit=", connectionPool.getConnectionDeficit(),
      ", churn=", connectionPool.getChurnedConnections());

        connection = connectionPool.getConnection();

    return connection;
  }

  /**
   * Get connection.
   * @param username The user name
   * @param password The password
   * @return The connection
   * @throws SQLException when unable to get a connection
   */
  public Connection getConnection(
    final String username,
    final String password) throws SQLException{
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Get parent logger.
   * @return The parent logger
   * @throws SQLFeatureNotSupportedException when unable to get the parent logger
   */
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException{
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Get log writer.
   * @return The log writer
   * @throws SQLException when unable to get the log writer
   */
  public PrintWriter getLogWriter() throws SQLException{
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Set log writer.
   * @param out The log writer
   * @throws SQLException when unable to set the log writer
   */
  public void setLogWriter(
    final PrintWriter out) throws SQLException{
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Get object that implements given interface.
   * @param iface The interface
   * @return The object that implements the interface
   * @throws SQLException when unable to obtain an object that implements the interface
   */
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> iface) throws SQLException{

        if (iface.isInstance(this) == true){
      return (T) this;
    }

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Indicate whether object implements given interface.
   * @param iface The interface
   * @return true if the object implements the given interface, false otherwise
   * @throws SQLException when unable to get the implementation indicator
   */
  public boolean isWrapperFor(Class<?> iface) throws SQLException{

        if (iface.isInstance(this) == true){
      return true;
    }

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Register driver.
   * @param driverClassName The driver class name
   */
  private void registerDriver(
    final String driverClassName){
        String methodName = "registerDriver";
    Driver driver;

        super.setDriverClassName(driverClassName);

    try{
            driver = (Driver) Class.forName(driverClassName).newInstance();
      DriverManager.registerDriver(driver);

      logger.debug(methodName, "Registered driver: ", driver.getClass().getName(), " [", driver.getMajorVersion(), ".", driver.getMinorVersion(), "]");
    }
    catch (Exception exception){
      throw new RuntimeException("Failed to register driver: " + exception.getMessage(), exception);
    }

  }

}
