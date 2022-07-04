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
import org.melior.service.exception.ApplicationException;
import org.melior.util.time.Timer;

/**
 * Implements a wrapper around a JDBC {@code ResultSet} delegate.  The result set writes
 * JDBC timing details to the logs during the lifetime of the result set.
 * @author Melior
 * @since 2.2
 */
public class ResultSet extends ServiceComponent implements InvocationHandler{
    private Connection connection;

    private java.sql.ResultSet delegate;

    private java.sql.ResultSet proxy;

    private Timer timer;

  /**
   * Constructor.
   * @param serviceContext The service context
   * @param connection The connection
   * @throws ApplicationException when the initialization fails
   */
  public ResultSet(
    final ServiceContext serviceContext,
    final Connection connection) throws ApplicationException{
        super(serviceContext);

        this.connection = connection;

        timer = Timer.ofNanos().start();
  }

  /**
   * Set result set.
   * @param resultSet The result set
   * @throws Exception when unable to create the proxy
   */
  void setResultSet(
    final java.sql.ResultSet resultSet) throws Exception{
        this.delegate = resultSet;

    try{
            proxy = (java.sql.ResultSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
        new Class[] {java.sql.ResultSet.class}, this);
    }
    catch (Exception exception){
      throw new Exception("Failed to create result set proxy: " + exception.getMessage(), exception);
    }

  }

  /**
   * Get proxy.
   * @return The proxy
   */
  java.sql.ResultSet getProxy(){
    return proxy;
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
    long duration;

        methodName = method.getName();

        if (methodName.startsWith("close") == true){
            invocationResult = invoke(method, args);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

      logger.debug(methodName, "Result set processed successfully.  Duration = ", duration, " ms");
    }
    else{
            invocationResult = invoke(method, args);
    }

    return invocationResult;
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

}
