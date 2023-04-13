/* __  __      _ _            
  |  \/  |    | (_)           
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
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.util.time.Timer;

/**
 * Implements a wrapper around a JDBC {@code ResultSet} delegate.  The result set writes
 * JDBC timing details to the logs during the lifetime of the result set.
 * @author Melior
 * @since 2.2
 */
public class ResultSet implements InvocationHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    private Connection connection;

    private java.sql.ResultSet delegate;

    private java.sql.ResultSet proxy;

    private Timer timer;

    /**
     * Constructor.
     * @param connection The connection
     * @param resultSet The result set
     * @throws ApplicationException if an error occurs during the construction
     */
    public ResultSet(
        final Connection connection,
        final java.sql.ResultSet resultSet) throws ApplicationException {

        super();

        this.connection = connection;

        this.delegate = resultSet;

        try {

            proxy = (java.sql.ResultSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[] {java.sql.ResultSet.class}, this);
        }
        catch (Exception exception) {
            throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Failed to create result set proxy: " + exception.getMessage(), exception);
        }

        timer = Timer.ofNanos().start();
    }

    /**
     * Get proxy.
     * @return The proxy
     */
    java.sql.ResultSet getProxy() {
        return proxy;
    }

    /**
     * Handle proxy invocation.
     * @param object The object on which the method was invoked
     * @param method The method that is to be invoked
     * @param args The arguments to invoke the method with
     * @return The result of the invocation
     * @throws Throwable if the invocation fails
     */
    public Object invoke(
        final Object object,
        final Method method,
        final Object[] args) throws Throwable {

        String methodName;
        Object invocationResult;
        long duration;

        methodName = method.getName();

        if (methodName.startsWith("close") == true) {

            invocationResult = invoke(method, args);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Result set processed successfully.  Duration = ", duration, " ms.");
        }
        else {

            invocationResult = invoke(method, args);
        }

        return invocationResult;
    }

    /**
     * Handle proxy invocation.
     * @param method The method to invoke
     * @param methodArgs The arguments to invoke with
     * @return The result of the invocation
     * @throws Throwable if the invocation fails
     */
    private Object invoke(
        final Method method,
        final Object[] methodArgs) throws Throwable {

        Object invocationResult;

        try {

            invocationResult = method.invoke(delegate, methodArgs);
        }
        catch (InvocationTargetException exception) {

            connection.captureException(exception.getCause());

            throw exception.getCause();
        }
        catch (Throwable exception) {

            connection.captureException(exception);

            throw exception;
        }

        return invocationResult;
    }

}
