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
import com.sun.rowset.CachedRowSetImpl;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.util.cache.Cache;
import org.melior.util.cache.SimpleCache;
import org.melior.util.string.StringUtil;

/**
 * Implements a wrapper around a JDBC {@code DatabaseMetaData} delegate.  If metadata
 * caching is enabled, then the results of all method calls on the delegate are cached.
 * @author Melior
 * @since 2.3
 */
@SuppressWarnings("restriction")
public class DatabaseMetaData implements InvocationHandler {

    private Connection connection;

    private java.sql.DatabaseMetaData delegate;

    private java.sql.DatabaseMetaData proxy;

    private SimpleCache<String, Object> cache;

    /**
     * Constructor.
     * @param connection The connection
     * @param metadata The database metadata
     * @throws ApplicationException if an error occurs during the construction
     */
    public DatabaseMetaData(
        final Connection connection,
        final java.sql.DatabaseMetaData metadata) throws ApplicationException {

        super();

        this.connection = connection;

        this.delegate = metadata;

        try {

            proxy = (java.sql.DatabaseMetaData) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[] {java.sql.DatabaseMetaData.class}, this);
        }
        catch (Exception exception) {
            throw new ApplicationException(ExceptionType.LOCAL_APPLICATION, "Failed to create metadata proxy: " + exception.getMessage(), exception);
        }

        cache = Cache.ofSimple(1000);
    }

    /**
     * Get proxy.
     * @return The proxy
     */
    java.sql.DatabaseMetaData getProxy() {
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
        String key;
        Object invocationResult;

        methodName = method.getName();

        if (methodName.equals("close") == true) {

            invocationResult = null;
        }
        else {

            key = methodName + "-" + StringUtil.join(args, "-");

            try {

                invocationResult = cache.get(key, () -> invoke(method, args));

                if (invocationResult instanceof javax.sql.rowset.CachedRowSet) {

                    ((javax.sql.rowset.CachedRowSet) invocationResult).beforeFirst();
                }

            }
            catch (ApplicationException exception) {
                throw exception.getCause().getCause();
            }

        }

        return invocationResult;
    }

    /**
     * Handle proxy invocation.
     * @param method The method to invoke
     * @param methodArgs The arguments to invoke with
     * @return The result of the invocation
     * @throws InvocationTargetException if the invocation fails
     */
    private Object invoke(
        final Method method,
        final Object[] methodArgs) throws InvocationTargetException {

        Object invocationResult;
        CachedRowSetImpl cachedRowSetImpl;
        CachedRowSet cachedRowSet;

        try {

            invocationResult = method.invoke(delegate, methodArgs);

            if (invocationResult instanceof java.sql.ResultSet) {

                try {

                    cachedRowSetImpl = new CachedRowSetImpl();
                    cachedRowSetImpl.setType(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE);
                    cachedRowSetImpl.populate((java.sql.ResultSet) invocationResult);
                    cachedRowSet = new CachedRowSet(connection, cachedRowSetImpl);
                }
                finally {

                    ((java.sql.ResultSet) invocationResult).close();
                }

                invocationResult = cachedRowSet.getProxy();
            }

        }
        catch (InvocationTargetException exception) {

            connection.captureException(exception.getCause());

            throw exception;
        }
        catch (Throwable exception) {

            connection.captureException(exception);

            throw new InvocationTargetException(exception);
        }

        return invocationResult;
    }

}
