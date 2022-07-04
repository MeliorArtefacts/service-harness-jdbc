/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.enhancer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.melior.jdbc.core.Statement;

/**
 * Interface for implementations that enhance SQL statements to provide
 * functionality that the JDBC driver, {@code DataSource} or underlying
 * database lacks.
 * <P>
 * Examples are, generating sequence numbers for a database that does
 * not support identity columns, sequence columns or table triggers,
 * and implementing {@code NULL} support for a database that does not
 * provide it, like Cassandra.
 * @author Melior
 * @since 2.2
 */
public interface StatementEnhancer{

  /**
   * Get and enhance statement.
   * @param dataSource The data source
   * @param connection The database connection
   * @param statementText The statement text
   * @param keyColumnNames The auto generated key column names
   * @return A statement that matches the statement text, or null if no statement could be found
   * @throws SQLException if a database access error occurs
   */
  public Statement getStatement(
    final DataSource dataSource,
    final Connection connection,
    final String statementText,
    final String[] keyColumnNames) throws SQLException;

  /**
   * Get system date and time.
   * @param dataSource The data source
   * @param connection The database connection
   * @return The system date and time
   * @throws SQLException if a database access error occurs
   */
  public Timestamp getSystemTimestamp(
    final DataSource dataSource,
    final Connection connection) throws SQLException;

  /**
   * Get system date.
   * @param dataSource The data source
   * @param connection The database connection
   * @return The system date
   * @throws SQLException if a database access error occurs
   */
  public Date getSystemDate(
    final DataSource dataSource,
    final Connection connection) throws SQLException;

}
