/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.core;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import org.melior.component.core.ServiceComponent;
import org.melior.context.service.ServiceContext;
import org.melior.jdbc.datasource.DataSource;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.util.cache.LRUCache;
import org.melior.util.string.StringUtil;
import org.melior.util.time.Timer;

/**
 * Implements a wrapper around a JDBC {@code Statement} delegate.  The statement writes
 * JDBC timing details to the logs during the lifetime of the statement.  If a statement
 * cache is enabled, then the statement will be pooled, unless the caller has marked
 * the statement as not poolable.
 * @author Melior
 * @since 2.2
 */
public class Statement extends ServiceComponent implements InvocationHandler{
    private DataSource dataSource;

    private Connection connection;

    private LRUCache<Object, Statement> statementCache;

    private Object text;

    private boolean cached;

    private java.sql.Statement delegate;

    private java.sql.Statement proxy;

    private StringBuilder arguments;

  /**
   * Constructor.
   * @param serviceContext The service context
   * @param dataSource The data source
   * @param connection The connection
   * @param statementCache The statement cache
   * @param statement The statement
   * @param text The statement text
   * @throws ApplicationException when the initialization fails
   */
  public Statement(
    final ServiceContext serviceContext,
    final DataSource dataSource,
    final Connection connection,
    final LRUCache<Object, Statement> statementCache,
    final java.sql.Statement statement,
    final String text) throws ApplicationException{
        super(serviceContext);

        Class proxyInterface;

        this.dataSource = dataSource;

        this.connection = connection;

        this.statementCache = statementCache;

        this.delegate = statement;

        this.text = text;

        proxyInterface = (statement instanceof java.sql.CallableStatement) ? java.sql.CallableStatement.class
      : (statement instanceof java.sql.PreparedStatement) ? java.sql.PreparedStatement.class
      : java.sql.Statement.class;

    try{
            proxy = (java.sql.Statement) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
        new Class[] {proxyInterface}, this);
    }
    catch (Exception exception){
      throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Failed to create statement proxy: " + exception.getMessage(), exception);
    }

        cached = false;
  }

  /**
   * Get proxy.
   * @return The proxy
   */
  java.sql.Statement getProxy(){
    return proxy;
  }

  /**
   * Destroy statement.
   */
  void destroy(){
        String methodName = "destroy";

    try{

            if (delegate != null){
        delegate.close();
      }

    }
    catch (Exception exception){
      logger.error(methodName, "Failed to close statement: ", exception.getMessage(), exception);
    }
    finally{
            statementCache = null;

            text = null;

            delegate = null;

            proxy = null;
    }

  }

  /**
   * Handle proxy invocation.
   * @param object The object on which the method was invoked
   * @param method The method that is to be invoked
   * @param args The arguments to invoke the method with
   * @return The result of the invocation
   * @throws Throwable when the invocation fails
   */
  public Object invoke(
    final Object object,
    final Method method,
    final Object[] args) throws Throwable{
        String methodName;
    Object invocationResult;
    ResultSet resultSet;

        methodName = method.getName();

        if (methodName.startsWith("set") == true){
            addArguments(methodName, args);

            invocationResult = invoke(method, args);
    }
        else if (methodName.startsWith("execute") == true){

            if (dataSource.getRequestTimeout() > 0){
                delegate.setQueryTimeout(dataSource.getRequestTimeout());
      }

            logArguments(methodName);

            invocationResult = invokeMeasured(method, methodName, args, "executed successfully", "execution failed");

            connection.setCommitPending((dataSource.isAutoCommit() == false) && (methodName.equals("executeQuery") == false));

            if (invocationResult instanceof java.sql.ResultSet){
                resultSet = new ResultSet(serviceContext, connection);
        resultSet.setResultSet((java.sql.ResultSet) invocationResult);

                invocationResult = resultSet.getProxy();
      }

    }
        else if (methodName.equals("close") == true){

            if (cached == false){

                if ((statementCache != null)
          && (statementCache.getCapacity() != 0)
          && (delegate.isPoolable() == true)){
                    statementCache.add(text, this);
          cached = true;
        }
        else{
                    destroy();
        }

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
  private void addArguments(
    final String methodName,
    final Object[] methodArgs){

        if ((dataSource.isLogArguments() == true) && (methodArgs != null)){
            arguments = (arguments == null) ? new StringBuilder() : arguments;

            arguments.append((arguments.length() > 0) ? ", " : "")
        .append("{").append(methodName.substring(3)).append(",")
        .append(StringUtil.join(methodArgs, ",")).append("}");
    }

  }

  /**
   * Log method arguments.
   * @param methodName The method name
   */
  private void logArguments(
    final String methodName){

        if ((dataSource.isLogArguments() == true) && (arguments != null)){
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
            connection.captureException(exception.getCause());

      throw exception.getCause();
    }
    catch (Throwable exception){
            connection.captureException(exception);

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

      logger.debug(methodName, "Statement ", successMessage, ".  Duration = ", duration, " ms");
    }
    catch (InvocationTargetException exception){
            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.error(methodName, "Statement ", failureMessage, ".  Duration = ", duration, " ms");

            connection.captureException(exception.getCause());

      throw exception.getCause();
    }
    catch (Throwable exception){
            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.error(methodName, "Statement ", failureMessage, ".  Duration = ", duration, " ms");

            connection.captureException(exception);

      throw exception;
    }

    return invocationResult;
  }

}
