/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.pool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.melior.component.core.ServiceComponent;
import org.melior.context.service.ServiceContext;
import org.melior.jdbc.core.Connection;
import org.melior.jdbc.core.SQLState;
import org.melior.jdbc.datasource.DataSource;
import org.melior.service.exception.ApplicationException;
import org.melior.util.collection.BlockingQueue;
import org.melior.util.number.Clamp;
import org.melior.util.number.Counter;
import org.melior.util.semaphore.Semaphore;
import org.melior.util.thread.DaemonThread;
import org.melior.util.thread.ThreadControl;
import org.melior.util.time.Timer;

/**
 * Implements a pool of JDBC {@code Connection} objects.
 * <p>
 * The pool adds additional {@code Connection} objects as and when demand requires,
 * but at the same time employs elision logic to ensure that the pool does not add
 * surplus {@code Connection} objects when the demand subsides following a surge.
 * <p>
 * The pool may also be configured to be bounded, in which case the pool will not
 * exceed its bounds when demand surges or subsides.
 * @author Melior
 * @since 2.2
 */
public class ConnectionPool extends ServiceComponent{
    protected DataSource dataSource;

    private String poolId;

    private ThreadLocal<Connection> threadIndex;

    private Counter totalConnections;

    private BlockingQueue<Connection> availableConnectionQueue;

    private Counter connectionsSupply;

    private Semaphore demandSemaphore;

    private int activeConnectionsCeiling;

    private BlockingQueue<Connection> retireConnectionQueue;

    private SQLException lastException;

    private long lastExceptionTime;

    private long backoffPeriod;

    private Counter churnedConnections;

    private long lastPruneTime;

  /**
   * Constructor.
   * @param serviceContext The service context
   * @param dataSource The data source
   * @throws ApplicationException when the initialization fails
   */
  public ConnectionPool(
    final ServiceContext serviceContext,
    final DataSource dataSource) throws ApplicationException{
        super(serviceContext);

        this.dataSource = dataSource;

        poolId = String.valueOf(this.hashCode());

        threadIndex = new ThreadLocal<Connection>();

        totalConnections = Counter.of(0);

        availableConnectionQueue = new BlockingQueue<Connection>();

        connectionsSupply = Counter.of(0);

        demandSemaphore = new Semaphore();

        activeConnectionsCeiling = 0;

        retireConnectionQueue = new BlockingQueue<Connection>();

        lastException = null;

        lastExceptionTime = 0;

        backoffPeriod = 0;

        churnedConnections = Counter.of(0);

        lastPruneTime = System.currentTimeMillis();

        resizePool();

        DaemonThread.create(() -> openNewConnections());

        DaemonThread.create(() -> pruneExpiredConnections());

        DaemonThread.create(() -> retireConnections());
  }

  /**
   * Get connection pool identifier.
   * @return The connection pool identifier
   */
  public final String getPoolId(){
    return poolId;
  }

  /**
   * Get total number of connections.
   * @return The total number of connections
   */
  public int getTotalConnections(){
    return totalConnections.get();
  }

  /**
   * Get number of active connections.
   * @return The number of active connections
   */
  public int getActiveConnections(){
    return totalConnections.get() - availableConnectionQueue.size();
  }

  /**
   * Get number of connections in deficit.
   * @return The number of connections in deficit
   */
  public int getConnectionDeficit(){
    return Math.abs(Clamp.clampInt(connectionsSupply.get(), Integer.MIN_VALUE, 0));
  }

  /**
   * Get number of churned connections.
   * @return The number of churned connections
   */
  public int getChurnedConnections(){
    return churnedConnections.get();
  }

  /**
   * Get connection to database.
   * @return The connection
   * @throws SQLException when unable to get a connection
   */
  public java.sql.Connection getConnection() throws SQLException{
        String methodName = "getConnection";
    boolean reuse;
    Connection connection;
    Timer timer;

        reuse = true;

        connection = threadIndex.get();

        if (connection == null){
            reuse = false;

            connectionsSupply.decrement();

            timer = Timer.ofMillis().start();

            while (true){

        try{
                    connection = availableConnectionQueue.remove(1, TimeUnit.MILLISECONDS);

                    if (connection == null){
                        demandSemaphore.release();

            logger.debug(methodName, "Wait for connection to become available.");

                        connection = availableConnectionQueue.remove((dataSource.getConnectionTimeout() * 1000) - timer.elapsedTime(), TimeUnit.MILLISECONDS);
          }

        }
        catch (Exception exception){
                    connectionsSupply.increment();

          throw new SQLException("Failed to get connection: " + exception.getMessage(), SQLState.CONNECTION_FAILURE.value(), exception);
        }

                if (connection == null){
                    connectionsSupply.increment();

          throw new SQLException("Timed out waiting for connection.", SQLState.CONNECTION_FAILURE.value());
        }

                if (connection.isValid(dataSource.isValidateOnBorrow()) == false){
          logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), "] is no longer valid and is being retired.");

                    connectionsSupply.decrement();

                    churnedConnections.increment();

                    totalConnections.decrement();

                    retireConnectionQueue.add(connection);
        }
                else if (connection.isEndOfLife() == true){
          logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), "] has reached end-of-life and is being retired.");

                    connectionsSupply.decrement();

                    totalConnections.decrement();

                    retireConnectionQueue.add(connection);
        }
        else{
          break;
        }

      }

    }

        connection.allocate(Thread.currentThread());

        activeConnectionsCeiling = Math.max(totalConnections.get() - availableConnectionQueue.size(), activeConnectionsCeiling);

        threadIndex.set(connection);

    logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), ", reuse=", reuse, "] allocated.");

    return connection.getProxy();
  }

  /**
   * Release connection into connection pool.
   * @param connection The connection to release
   * @throws SQLException when unable to release the connection
   */
  public void releaseConnection(
    final Connection connection) throws SQLException{
        String methodName = "releaseConnection";

        threadIndex.set(null);

        connection.release(Thread.currentThread());

        if (connection.isValid(false) == false){
      logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), "] is no longer valid and is being retired.");

            churnedConnections.increment();

            totalConnections.decrement();

            retireConnectionQueue.add(connection);
    }
    else{
            connectionsSupply.increment();

            availableConnectionQueue.add(connection);

      logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), "] released.");
    }

  }

  /**
   * Open new connections.
   */
  protected void openNewConnections(){
        String methodName = "openNewConnections";
    long remainingBackoff;
    Connection connection;

        while (isActive() == true){

      try{
                demandSemaphore.acquire();

                while (((connectionsSupply.get() < 0)
          || (totalConnections.get() < dataSource.getMinimumConnections()))
          && (totalConnections.get() < dataSource.getMaximumConnections())){

                    if (lastException != null){
                        remainingBackoff = backoffPeriod - (System.currentTimeMillis() - lastExceptionTime);

                        if (remainingBackoff > 0){
              logger.debug(methodName, "Backing off for ", (remainingBackoff / 1000), " seconds.");

                            ThreadControl.sleep(remainingBackoff);

              continue;
            }

          }

          try{
                        connection = new Connection(serviceContext, dataSource, this);
            connection.open();

                        totalConnections.increment();

                        connectionsSupply.increment();

                        availableConnectionQueue.add(connection);

                        lastException = null;

                        backoffPeriod = 0;
          }
          catch (Exception exception){
            logger.error(methodName, "Failed to open connection: ", exception.getMessage(), exception);

                        captureException(exception);

                        backoffPeriod = (backoffPeriod == 0) ? dataSource.getBackoffPeriod() : Clamp.clampLong(
              (long) (backoffPeriod * dataSource.getBackoffMultiplier()), 0, dataSource.getBackoffLimit());
          }

        }

      }
      catch (Exception exception){
        logger.error(methodName, "Failed to open new connections: ", exception.getMessage(), exception);
      }

    }

  }

  /**
   * Periodically prune expired connections.
   */
  public void pruneExpiredConnections(){
        String methodName = "pruneExpiredConnections";
    Connection connection;

        while ((isActive() == true) && (dataSource.getInactivityTimeout() > 0) && (dataSource.getPruneInterval() > 0)){

      try{

                if ((System.currentTimeMillis() - lastPruneTime) > dataSource.getInactivityTimeout()){
                    lastPruneTime = System.currentTimeMillis();

                    while (totalConnections.get() > Math.max(dataSource.getMinimumConnections(), activeConnectionsCeiling)){
                        connection = availableConnectionQueue.remove(1, TimeUnit.MILLISECONDS);

                        if (connection == null){
              break;
            }

            logger.debug(methodName, "Connection [", connection.getConnectionDescriptor(), "] has expired and is being retired.");

                        connectionsSupply.decrement();

                        totalConnections.decrement();

                        retireConnectionQueue.add(connection);
          }

                    activeConnectionsCeiling = 0;
        }

                ThreadControl.wait(this, dataSource.getPruneInterval());
      }
      catch (Exception exception){
        logger.error(methodName, "Failed to prune expired connections: ", exception.getMessage(), exception);
      }

    }

  }

  /**
   * Retire connections.
   */
  private void retireConnections(){
        String methodName = "retireConnections";
    Connection connection;

        while (isActive() == true){

      try{
                connection = retireConnectionQueue.remove();

                connection.close();
      }
      catch (Exception exception){
        logger.error(methodName, "Failed to retire connections: ", exception.getMessage(), exception);
      }

    }

  }

  /**
   * Resize connection pool to fit dimensions.
   */
  public void resizePool(){
        String methodName = "resizePool";

    try{

            if (totalConnections.get() < dataSource.getMinimumConnections()){
        logger.debug(methodName, "Connection pool [", poolId, "] resized to fit dimensions.");

                demandSemaphore.release();
      }

    }
    catch (Exception exception){
      logger.error(methodName, "Failed to resize connection pool: ", exception.getMessage(), exception);
    }

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

        lastExceptionTime = System.currentTimeMillis();
  }

}
