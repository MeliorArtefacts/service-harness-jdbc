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
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;

/**
 * Implements a wrapper around a JDBC {@code CachedRowSet} delegate.
 * @author Melior
 * @since 2.3
 */
public class CachedRowSet implements InvocationHandler {

    private Connection connection;

    private javax.sql.rowset.CachedRowSet delegate;

    private javax.sql.rowset.CachedRowSet proxy;

    /**
     * Constructor.
     * @param connection The connection
     * @param cachedRowSet The cached row set
     * @throws ApplicationException if an error occurs during the construction
     */
    public CachedRowSet(
        final Connection connection,
        final javax.sql.rowset.CachedRowSet cachedRowSet) throws ApplicationException {

        super();

        this.connection = connection;

        this.delegate = cachedRowSet;

        try {

            proxy = (javax.sql.rowset.CachedRowSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[] {javax.sql.rowset.CachedRowSet.class}, this);
        }
        catch (Exception exception) {
            throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Failed to create cached row set proxy: " + exception.getMessage(), exception);
        }

    }

    /**
     * Get proxy.
     * @return The proxy
     */
    javax.sql.rowset.CachedRowSet getProxy() {
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

        methodName = method.getName();

        if (methodName.startsWith("close") == true) {

            invocationResult = null;
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
