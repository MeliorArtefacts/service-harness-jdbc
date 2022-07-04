/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.core;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.melior.component.core.ServiceComponent;
import org.melior.context.service.ServiceContext;
import org.melior.jdbc.datasource.DataSource;
import org.melior.jdbc.exception.SQLExceptionMapper;
import org.melior.jdbc.pool.ConnectionPool;
import org.melior.jdbc.session.SessionData;
import org.melior.jdbc.util.TimeDelta;
import org.melior.jdbc.session.SessionController;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.util.cache.LRUCache;
import org.melior.util.string.StringUtil;
import org.melior.util.time.Timer;

/**
 * Implements a wrapper around a JDBC {@code Connection} delegate.  The connection writes
 * JDBC timing details to the logs during the lifetime of the connection.  The connection
 * is pooled until it experiences a database access error, or until it expires, either
 * due to being in surplus at the timeout interval, or due to it reaching the maximum
 * lifetime for a connection.
 * @author Melior
 * @since 2.2
 */
public class Connection extends ServiceComponent implements InvocationHandler{
    private DataSource dataSource;

    private ConnectionPool connectionPool;

    private String connectionId;

    private Timer lifetimeTimer;

    private SessionData sessionData;

    private TimeDelta timeDelta;

    private LRUCache<Object, Statement> statementCache;

    private String connectionDescriptor;

    private Thread ownerThread;

    private boolean commitPending;

    private SQLException lastException;

    private java.sql.Connection delegate;

    private java.sql.Connection proxy;

    private boolean validationSupported;

    private StringBuilder arguments;

  /**
   * Constructor.
   * @param serviceContext The service context
   * @param dataSource The data source
   * @param connectionPool The connection pool
   * @throws ApplicationException when the initialization fails
   */
  public Connection(
    final ServiceContext serviceContext,
    final DataSource dataSource,
    final ConnectionPool connectionPool) throws ApplicationException{
        super(serviceContext);

        this.dataSource = dataSource;

        this.connectionPool = connectionPool;

        connectionId = String.valueOf(this.hashCode());

        lifetimeTimer = Timer.ofMillis().start();

        sessionData = null;

        timeDelta = dataSource.getTimeDelta();

        statementCache = new LRUCache<Object, Statement>(dataSource.getStatementCacheSize());

        setConnectionDescriptor();

        commitPending = false;

        ownerThread = null;

        lastException = null;

        delegate = null;

    try{
            proxy = (java.sql.Connection) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
        new Class[] {java.sql.Connection.class}, this);
    }
    catch (Exception exception){
      throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Failed to create connection proxy: " + exception.getMessage(), exception);
    }

        arguments = null;
  }

  /**
   * Get connection descriptor.
   * @return The connection descriptor
   */
  public String getConnectionDescriptor(){
    return connectionDescriptor;
  }

  /**
   * Get proxy.
   * @return The proxy
   */
  public java.sql.Connection getProxy(){
    return proxy;
  }

  /**
   * Allocate connection to specified thread.
   * @param thread The thread that will own the connection
   */
  public void allocate(
    final Thread thread){
        ownerThread = thread;

        lastException = null;
  }

  /**
   * Release connection from specified thread.
   * @param thread The thread that owns the connection
   * @throws SQLException when the connection has already been released
   */
  public void release(
    final Thread thread) throws SQLException{

        if ((ownerThread == null) || (ownerThread != thread)){
      throw new SQLException("Connection has already been released.  Consider passing connection between methods.", SQLState.CONNECTION_INVALID.value());
    }

        ownerThread = null;
  }

  /**
   * Check whether connection is still valid.
   * @param fullValidation The full validation indicator
   * @return true if the connection is still valid, false otherwise
   */
  public boolean isValid(
    final boolean fullValidation){
        String methodName = "isValid";
    ExceptionType exceptionType;

        if (lastException != null){
            exceptionType = SQLExceptionMapper.map(lastException);

            if ((exceptionType == ExceptionType.DATAACCESS_COMMUNICATION)
        || (exceptionType == ExceptionType.DATAACCESS_SYSTEM)){
        return false;
      }

    }

        if ((fullValidation == true) && (validationSupported == true)){
      logger.debug(methodName, "Connection [", connectionDescriptor, "] is being validated.");

      try{
                return delegate.isValid(dataSource.getValidationTimeout());
      }
      catch (Exception exception){
        return false;
      }

    }

    return true;
  }

  /**
   * Check whether connection has reached end-of-life
   * @return true if the connection has reached end-of-life, false otherwise
   */
  public boolean isEndOfLife(){
    return (dataSource.getMaximumLifetime() > 0)
      && (lifetimeTimer.elapsedTime() > dataSource.getMaximumLifetime());
  }

  /**
   * Open connection.
   * @throws SQLException when the open attempt fails
   */
  public void open() throws SQLException{
        String methodName = "open";
    SessionController sessionController;
    Timer timer;
    long duration;

        timer = Timer.ofNanos().start();

    try{
      logger.debug(methodName, "Connection [", connectionDescriptor, "] attempting to open.  URL = ", dataSource.getUrl());

            DriverManager.setLoginTimeout(dataSource.getConnectionTimeout());
      delegate = DriverManager.getConnection(dataSource.getUrl(), dataSource.getConnectionProperties());

      try{
                configureConnection(dataSource, delegate);

                sessionController = dataSource.getSessionController();

                if (sessionController != null){
                    sessionData = sessionController.prepareSession(dataSource, delegate);

                    timeDelta.setDelta(sessionData.getTimeDelta());

                    setConnectionDescriptor();
        }

      }
      catch (Exception exception){

        try{
                    delegate.close();
        }
        catch (Exception exception2){
                    sessionData = null;

                    delegate = null;
        }

        throw exception;
      }

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.debug(methodName, "Connection [", connectionDescriptor, "] opened successfully.  Duration = ", duration, " ms");
    }
    catch (Exception exception){
            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.error(methodName, "Connection [", connectionDescriptor, "] open attempt failed.  Duration = ", duration, " ms");

            captureException(exception);

      throw exception;
    }

  }

  /**
   * Close connection.
   */
  public void close(){
        String methodName = "close";

    try{

            if ((delegate != null) && (delegate.isClosed() == false)){
                delegate.close();
      }

      logger.debug(methodName, "Connection [", connectionDescriptor, "] closed successfully.");
    }
    catch (Exception exception){
      logger.error(methodName, "Connection [", connectionDescriptor, "] close attempt failed.", exception);
    }
    finally{
            sessionData = null;

            statementCache.clear();

            delegate = null;

            setConnectionDescriptor();
    }

  }

  /**
   * Configure connection.
   * @param dataSource The data source
   * @param connection The connection
   */
  private void configureConnection(
    final DataSource dataSource,
    final java.sql.Connection connection){
        String methodName = "configureConnection";

        if (dataSource.getCatalog() != null){

      try{
                delegate.setCatalog(dataSource.getCatalog());
      }
      catch (SQLException exception){
        logger.warn(methodName, "Driver does not support setting catalog.");
      }

    }

        if (dataSource.getSchema() != null){

      try{
                delegate.setSchema(dataSource.getSchema());
      }
      catch (SQLException exception){
        logger.warn(methodName, "Driver does not support setting schema.");
      }

    }

    try{
            delegate.setReadOnly(dataSource.isReadOnly());
    }
    catch (SQLException exception){
      logger.warn(methodName, "Driver does not support setting read-only mode.");
    }

        if (dataSource.getTransactionIsolation() != null){

      try{
                delegate.setTransactionIsolation(dataSource.getTransactionIsolation().value());
      }
      catch (SQLException exception){
        logger.warn(methodName, "Driver does not support setting transaction isolation.");
      }

    }

    try{
            delegate.setAutoCommit(dataSource.isAutoCommit());
    }
    catch (SQLException exception){
      logger.warn(methodName, "Driver does not support setting auto-commit mode.");
    }

        if (serviceContext.getServiceName() != null){

      try{
                delegate.setClientInfo("ApplicationName", serviceContext.getServiceName());
      }
      catch (SQLException exception){

        try{
                    delegate.setClientInfo("OCSID.CLIENTID", serviceContext.getServiceName());
        }
        catch (SQLException exception2){
          logger.warn(methodName, "Driver does not support setting client info.");
        }

      }

    }

    try{
            delegate.isValid(dataSource.getValidationTimeout());

            validationSupported = true;
    }
    catch (SQLException exception){
      logger.warn(methodName, "Driver does not support connection validation.");

            validationSupported = false;
    }

  }

  /**
   * Build connection descriptor.
   */
  private void setConnectionDescriptor(){
        connectionDescriptor = "id=" + connectionId + ((sessionData == null) ? ""
      : ", session=" + sessionData.getSessionId() + ", delta=" + timeDelta.getDelta() + " ms");
  }

  /**
   * Indicate whether transaction commit is pending.
   * @param commitPending true if a transaction commit is pending, false otherwise
   */
  public void setCommitPending(
    final boolean commitPending){
    this.commitPending = commitPending;
  }

  /**
   * Capture last exception.
   * @param exception The last exception
   */
  void captureException(
    final Throwable exception){
        lastException = (exception instanceof SQLException) ? (SQLException) exception : (exception instanceof IOException) ?
      new SQLException(exception.getMessage(), SQLState.CONNECTION_FAILURE.value(), exception) :
      new SQLException(exception.getMessage(), SQLState.DYNAMIC_SQL_ERROR.value(), exception);
  }

  /**
   * Handle proxy invocation.
   * @param object The object on which the method was invoked
   * @param method The method to invoke
   * @param args The arguments to invoke with
   * @return The result of the invocation
   * @throws Throwable when the invocation fails
   */
  public Object invoke(
    final Object object,
    final Method method,
    final Object[] args) throws Throwable{
        String methodName;
    Class[] methodParameterTypes;
    Statement statement;
    Object invocationResult;
    Method rollbackMethod;

        methodName = method.getName();

        logArguments(methodName, args);

        if ((methodName.equals("prepareStatement") == true) || (methodName.equals("prepareCall") == true)){
            methodParameterTypes = method.getParameterTypes();

            if ((statementCache.getCapacity() > 0) && (methodParameterTypes.length > 0)
        && (methodParameterTypes[0] == String.class)){
                statementCache.setCapacity(dataSource.getStatementCacheSize());

                statement = statementCache.get(args[0]);

                if (statement != null){
                    invocationResult = statement.getProxy();

          logger.debug(methodName, "Connection [", connectionDescriptor, "] using cached statement.");
        }
        else{
                    invocationResult = invokeMeasured(method, methodName, args, "statement prepared successfully", "statement prepare failed");

                    statement = new Statement(serviceContext, dataSource, this, statementCache, (java.sql.Statement) invocationResult, (String) args[0]);

                    invocationResult = statement.getProxy();
        }

      }
      else{
                invocationResult = invokeMeasured(method, methodName, args, "statement prepared successfully", "statement prepare failed");

                statement = new Statement(serviceContext, dataSource, this, null, (java.sql.Statement) invocationResult, null);

                invocationResult = statement.getProxy();
      }

    }
        else if (methodName.equals("createStatement") == true){
            invocationResult = invokeMeasured(method, methodName, args, "statement created successfully", "statement create failed");

            statement = new Statement(serviceContext, dataSource, this, null, (java.sql.Statement) invocationResult, null);

            invocationResult = statement.getProxy();
    }
        else if (methodName.equals("getMetaData") == true){
            invocationResult = invokeMeasured(method, methodName, args, "metadata retrieved successfully", "metadata retrieval failed");

            commitPending = false;
    }
        else if (methodName.equals("commit") == true){
            invocationResult = invokeMeasured(method, methodName, args, "transaction committed successfully", "transaction commit failed");

            commitPending = false;
    }
        else if (methodName.equals("rollback") == true){
            invocationResult = invokeMeasured(method, methodName, args, "transaction rolled back successfully", "transaction rollback failed");

            commitPending = false;
    }
        else if (methodName.equals("close") == true){

      try{

                if (commitPending == true){
                    rollbackMethod = delegate.getClass().getMethod("rollback", new Class[0]);

                    invocationResult = invokeMeasured(rollbackMethod, rollbackMethod.getName(), new Object[0], "transaction rolled back successfully", "transaction rollback failed");

                    commitPending = false;

                    throw new SQLException("Executed forced rollback because transaction was left uncommitted.");
        }

      }
      finally{

        try{
                    delegate.clearWarnings();
        }
        catch (Exception exception){
        }

                connectionPool.releaseConnection(this);
      }

            invocationResult = null;
    }
    else{
            invocationResult = invoke(method, args);
    }

    return invocationResult;
  }

  /**
   * Log method arguments.
   * @param methodName The method name
   * @param methodArgs The method arguments
   */
  private void logArguments(
    final String methodName,
    final Object[] methodArgs){

        if ((dataSource.isLogArguments() == true) && (methodArgs != null)){
            arguments = (arguments == null) ? new StringBuilder() : arguments;

            arguments.append("{").append(StringUtil.join(methodArgs, "}, {")).append("}");

            logger.debug(methodName, "arguments = ", arguments.toString());

            arguments.delete(0, arguments.length());
    }

  }

  /**
   * Handle proxy invocation.
   * @param method The method to invoke
   * @param methodArgs The arguments to invoke with
   * @return The result of the invocation
   * @throws Throwable when the invocation fails
   */
  private Object invoke(
    final Method method,
    final Object[] methodArgs) throws Throwable{
        Object invocationResult;

    try{
            invocationResult = method.invoke(delegate, methodArgs);
    }
    catch (InvocationTargetException exception){
            captureException(exception.getCause());

      throw exception.getCause();
    }
    catch (Throwable exception){
            captureException(exception);

      throw exception;
    }

    return invocationResult;
  }

  /**
   * Handle proxy invocation.
   * @param method The method to invoke
   * @param methodName The method name
   * @param methodArgs The arguments to invoke with
   * @param successMessage The message to log on success
   * @param failureMessage The message to log on failure
   * @return The result of the invocation
   * @throws Throwable when the invocation fails
   */
  private Object invokeMeasured(
    final Method method,
    final String methodName,
    final Object[] methodArgs,
    final String successMessage,
    final String failureMessage) throws Throwable{
        Timer timer;
    Object invocationResult;
    long duration;

        timer = Timer.ofNanos().start();

    try{
            invocationResult = method.invoke(delegate, methodArgs);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.debug(methodName, "Connection [", connectionDescriptor, "] ", successMessage, ".  Duration = ", duration, " ms");
    }
    catch (InvocationTargetException exception){
            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.error(methodName, "Connection [", connectionDescriptor, "] ", failureMessage, ".  Duration = ", duration, " ms");

            captureException(exception.getCause());

      throw exception.getCause();
    }
    catch (Throwable exception){
            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.error(methodName, "Connection [", connectionDescriptor, "] ", failureMessage, ".  Duration = ", duration, " ms");

            captureException(exception);

      throw exception;
    }

    return invocationResult;
  }

}
