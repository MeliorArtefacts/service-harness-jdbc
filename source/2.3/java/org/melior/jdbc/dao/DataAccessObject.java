/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.dao;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import org.melior.component.core.ServiceComponent;
import org.melior.context.service.ServiceContext;
import org.melior.jdbc.datasource.DataSource;
import org.melior.jdbc.enhancer.StatementEnhancer;
import org.melior.jdbc.util.TimeDelta;
import org.melior.service.exception.ApplicationException;
import org.melior.util.time.DateFormatter;
import org.melior.util.time.DateParser;

/**
 * Base class for DAO implementations.  Provides helper functions that
 * make writing JDBC code simpler and more resilient.
 * @author Melior
 * @since 2.2
 */
public abstract class DataAccessObject extends ServiceComponent{
    protected javax.sql.DataSource dataSource;

    private StatementEnhancer statementEnhancer;

    private TimeDelta timeDelta;

  /**
   * Constructor.
   * @param serviceContext The service context
   * @param dataSource The data source
   * @throws ApplicationException if an error occurs during the construction
   */
  public DataAccessObject(
    final ServiceContext serviceContext,
    final javax.sql.DataSource dataSource) throws ApplicationException{
        super(serviceContext);

        this.dataSource = dataSource;

        statementEnhancer = (dataSource instanceof DataSource) ? ((DataSource) dataSource).getStatementEnhancer() : null;

        timeDelta = (dataSource instanceof DataSource) ? ((DataSource) dataSource).getTimeDelta() : null;
  }

  /**
   * Returns the first generated numeric key from the statement.
   * @param statement The statement
   * @return The generated key
   * @throws SQLException if no generated keys are available, or when the generated key is not numeric
   */
  protected long getFirstGeneratedKey(
    final PreparedStatement statement) throws SQLException{
    return getGeneratedKeys(statement)[0];
  }

  /**
   * Returns the generated numeric keys from the statement.
   * @param statement The statement
   * @return The generated keys
   * @throws SQLException if no generated keys are available, or when a generated key is not numeric
   */
  protected long[] getGeneratedKeys(
    final PreparedStatement statement) throws SQLException{
        ResultSet resultSet;
    ResultSetMetaData resultSetMetaData;
    long[] keys;

        resultSet = statement.getGeneratedKeys();

    try{

            if (resultSet.next() == false){
        throw new SQLException("No generated keys available.");
      }

            resultSetMetaData = resultSet.getMetaData();

            keys = new long[resultSetMetaData.getColumnCount()];

      for (int i = 0; i < keys.length; i++){
        keys[i] = resultSet.getLong(i + 1);
      }

    }
    finally{
            resultSet.close();
    }

    return keys;
  }

  /**
   * Parse timestamp from string.
   * @param string The string to parse
   * @return The timestamp
   * @throws Exception when unable to parse the input string
   */
  protected Timestamp parseTimestamp(
    final String string) throws Exception{

        if ((string == null) || (string.length() == 0)){
      return null;
    }

        return Timestamp.valueOf(string);
  }

  /**
   * Parse timestamp from string.
   * @param string The string to parse
   * @param customFormat The custom format
   * @return The timestamp
   * @throws Exception when unable to parse the input string
   */
  protected Timestamp parseTimestamp(
    final String string,
    final String customFormat) throws Exception{

        if ((string == null) || (string.length() == 0)){
      return null;
    }

        return Timestamp.valueOf(DateParser.parseTimestamp(string, customFormat));
  }

  /**
   * Format timestamp to string.
   * @param timestamp The timestamp to format
   * @return The string
   */
  protected String formatTimestamp(
    final Timestamp timestamp){

        if (timestamp == null){
      return null;
    }

        return DateFormatter.formatTimestamp(timestamp.toLocalDateTime());
  }

  /**
   * Format timestamp to string.
   * @param timestamp The timestamp to format
   * @param customFormat The custom format
   * @return The string
   */
  protected String formatTimestamp(
    final Timestamp timestamp,
    final String customFormat){

        if (timestamp == null){
      return null;
    }

        return DateFormatter.formatTimestamp(timestamp.toLocalDateTime(), customFormat);
  }

  /**
   * Adjust timestamp with time period.
   * @param timestamp The timestamp
   * @param period The time period
   * @return The new timestamp
   */
  public Timestamp adjustTimestamp(
    final Timestamp timestamp,
    final Duration period){

        if (timestamp == null){
      return null;
    }

        return Timestamp.valueOf(timestamp.toLocalDateTime().plus(period));
  }

  /**
   * Returns the value of the specified {@code Timestamp} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getTimestampAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        Timestamp value;

        value = resultSet.getTimestamp(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : formatTimestamp(value);
  }

  /**
   * Returns the value of the specified {@code Timestamp} column in the result set as a
   * {@code LocalDateTime}.  If the column is SQL {@code NULL}, then {@code null} is
   * returned.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as a {@code LocalDateTime}, if the column is not SQL {@code NULL}, otherwise {@code null}
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected LocalDateTime getTimestampAsLocal(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        Timestamp value;

        value = resultSet.getTimestamp(columnIndex);
    return (resultSet.wasNull() == true) ? null : value.toLocalDateTime();
  }

  /**
   * Returns the value of the specified {@code Timestamp} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getTimestampAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        Timestamp value;

        value = statement.getTimestamp(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : formatTimestamp(value);
  }

  /**
   * Returns the value of the specified {@code Timestamp} column in the statement as a
   * {@code LocalDateTime}.  If the column is SQL {@code NULL}, then {@code null} is
   * returned.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise {@code null}
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected LocalDateTime getTimestampAsLocal(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        Timestamp value;

        value = statement.getTimestamp(columnIndex);
    return (statement.wasNull() == true) ? null : value.toLocalDateTime();
  }

  /**
   * Sets the specified {@code Timestamp} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setTimestamp(
    final PreparedStatement statement,
    final int columnIndex,
    final Timestamp value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.TIMESTAMP);
    }
    else{
      statement.setTimestamp(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code LocalDateTime} as a {@code Timestamp} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setTimestamp(
    final PreparedStatement statement,
    final int columnIndex,
    final LocalDateTime value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.TIMESTAMP);
    }
    else{
      statement.setTimestamp(columnIndex, Timestamp.valueOf(value));
    }

  }

  /**
   * Returns the current system date and time.  If a statement enhancer is enabled, then
   * the system date and time is retrieved from the database to which the connection is
   * connected, otherwise the system date and time is generated from the local time source.
   * @param connection The database connection
   * @return The system date
   * @throws SQLException if a database access error occurs
   */
  protected Timestamp getSystemTimestamp(
    final Connection connection) throws SQLException{
        Timestamp timestamp;

        timestamp = null;

        if (statementEnhancer != null){
            timestamp = statementEnhancer.getSystemTimestamp(dataSource, connection);
    }

        if (timestamp == null){
            timestamp = Timestamp.valueOf(LocalDateTime.now().plus(Duration.ofMillis((timeDelta == null) ? 0 : timeDelta.getDelta())));
    }

    return timestamp;
  }

  /**
   * Parse date from string.
   * @param string The string to parse
   * @return The date
   * @throws Exception when unable to parse the string
   */
  protected Date parseDate(
    final String string) throws Exception{

        if ((string == null) || (string.length() == 0)){
      return null;
    }

        return Date.valueOf(string);
  }

  /**
   * Parse date from string.
   * @param string The string to parse
   * @param customFormat The custom format
   * @return The date
   * @throws Exception when unable to parse the string
   */
  protected Date parseDate(
    final String string,
    final String customFormat) throws Exception{

        if ((string == null) || (string.length() == 0)){
      return null;
    }

        return Date.valueOf(DateParser.parseDate(string, customFormat));
  }

  /**
   * Format date to string.
   * @param date The date to format
   * @return The string
   */
  protected String formatDate(
    final Date date){

        if (date == null){
      return null;
    }

        return DateFormatter.formatDate(date.toLocalDate());
  }

  /**
   * Format date to string.
   * @param date The date to format
   * @param customFormat The custom format
   * @return The string
   */
  protected String formatDate(
    final Date date,
    final String customFormat){

        if (date == null){
      return null;
    }

        return DateFormatter.formatDate(date.toLocalDate(), customFormat);
  }

  /**
   * Adjust date with time period.
   * @param date The date
   * @param period The time period
   * @return The new date
   */
  public Date adjustDate(
    final Date date,
    final Period period){

        if (date == null){
      return null;
    }

        return Date.valueOf(date.toLocalDate().plus(period));
  }

  /**
   * Returns the value of the specified {@code Date} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getDateAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        Date value;

        value = resultSet.getDate(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : formatDate(value);
  }

  /**
   * Returns the value of the specified {@code Date} column in the result set as a
   * {@code LocalDate}.  If the column is SQL {@code NULL}, then {@code null} is
   * returned.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise {@code null}
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected LocalDate getDateAsLocal(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        Date value;

        value = resultSet.getDate(columnIndex);
    return (resultSet.wasNull() == true) ? null : value.toLocalDate();
  }

  /**
   * Returns the value of the specified {@code Date} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getDateAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        Date value;

        value = statement.getDate(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : formatDate(value);
  }

  /**
   * Returns the value of the specified {@code Date} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then {@code null} is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise {@code null}
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected LocalDate getDateAsLocal(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        Date value;

        value = statement.getDate(columnIndex);
    return (statement.wasNull() == true) ? null : value.toLocalDate();
  }

  /**
   * Sets the specified {@code Date} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setDate(
    final PreparedStatement statement,
    final int columnIndex,
    final Date value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.DATE);
    }
    else{
      statement.setDate(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code LocalDate} as a {@code Date} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setDate(
    final PreparedStatement statement,
    final int columnIndex,
    final LocalDate value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.DATE);
    }
    else{
      statement.setDate(columnIndex, Date.valueOf(value));
    }

  }

  /**
   * Returns the current system date.  If a statement enhancer is enabled, then
   * the system date is retrieved from the database to which the connection is
   * connected, otherwise the system date is generated using the local time source.
   * @param connection The database connection
   * @return The system date
   * @throws SQLException if a database access error occurs
   */
  protected Date getSystemDate(
    final Connection connection) throws SQLException{
        Date date;

        date = null;

        if (statementEnhancer != null){
            date = statementEnhancer.getSystemDate(dataSource, connection);
    }

        if (date == null){
            date = Date.valueOf(LocalDate.now().plus(Duration.ofMillis((timeDelta == null) ? 0 : timeDelta.getDelta())));
    }

    return date;
  }

  /**
   * Returns the value of the specified {@code int} column in the result set.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected int getInt(
    final ResultSet resultSet,
    final int columnIndex,
    final int defaultValue) throws SQLException{
        int value;

        value = resultSet.getInt(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code int} column in the result set as an
   * {@code Integer}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as an {@code Integer}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected Integer getBoxedInt(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        int value;

        value = resultSet.getInt(columnIndex);
    return (resultSet.wasNull() == true) ? null : Integer.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code int} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getIntAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        int value;

        value = resultSet.getInt(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code int} column in the statement.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected int getInt(
    final CallableStatement statement,
    final int columnIndex,
    final int defaultValue) throws SQLException{
        int value;

        value = statement.getInt(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code int} column in the statement as an
   * {@code Integer}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as an {@code Integer}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected Integer getBoxedInt(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        int value;

        value = statement.getInt(columnIndex);
    return (statement.wasNull() == true) ? null : Integer.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code int} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getIntAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        int value;

        value = statement.getInt(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Sets the specified {@code int} column in the statement.
   * If the value is 0, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setInt(
    final PreparedStatement statement,
    final int columnIndex,
    final int value) throws SQLException{

        if (value == 0){
      statement.setNull(columnIndex, Types.INTEGER);
    }
    else{
      statement.setInt(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code int} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setInt(
    final PreparedStatement statement,
    final int columnIndex,
    final Integer value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.INTEGER);
    }
    else{
      statement.setInt(columnIndex, value.intValue());
    }

  }

  /**
   * Returns the value of the specified {@code long} column in the result set.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected long getLong(
    final ResultSet resultSet,
    final int columnIndex,
    final long defaultValue) throws SQLException{
        long value;

        value = resultSet.getLong(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code long} column in the result set as a
   * {@code Long}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as a {@code Long}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected Long getBoxedLong(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        long value;

        value = resultSet.getLong(columnIndex);
    return (resultSet.wasNull() == true) ? null : Long.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code long} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getLongAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        long value;

        value = resultSet.getLong(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code long} column in the statement.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected long getLong(
    final CallableStatement statement,
    final int columnIndex,
    final long defaultValue) throws SQLException{
        long value;

        value = statement.getLong(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code long} column in the statement as a
   * {@code Long}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as a {@code Long}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected Long getBoxedLong(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        long value;

        value = statement.getLong(columnIndex);
    return (statement.wasNull() == true) ? null : Long.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code long} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getLongAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        long value;

        value = statement.getLong(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Sets the specified {@code long} column in the statement.
   * If the value is 0, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setLong(
    final PreparedStatement statement,
    final int columnIndex,
    final long value) throws SQLException{

        if (value == 0){
      statement.setNull(columnIndex, Types.BIGINT);
    }
    else{
      statement.setLong(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code long} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setLong(
    final PreparedStatement statement,
    final int columnIndex,
    final Long value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.BIGINT);
    }
    else{
      statement.setLong(columnIndex, value.longValue());
    }

  }

  /**
   * Returns the value of the specified {@code float} column in the result set.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected float getFloat(
    final ResultSet resultSet,
    final int columnIndex,
    final float defaultValue) throws SQLException{
        float value;

        value = resultSet.getFloat(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code float} column in the result set as a
   * {@code Float}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as a {@code Float}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected Float getBoxedFloat(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        float value;

        value = resultSet.getFloat(columnIndex);
    return (resultSet.wasNull() == true) ? null : Float.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code float} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getFloatAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        float value;

        value = resultSet.getFloat(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code float} column in the statement.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected float getFloat(
    final CallableStatement statement,
    final int columnIndex,
    final float defaultValue) throws SQLException{
        float value;

        value = statement.getFloat(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code float} column in the statement as a
   * {@code Float}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as a {@code Float}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected Float getBoxedFloat(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        float value;

        value = statement.getFloat(columnIndex);
    return (statement.wasNull() == true) ? null : Float.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code float} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getFloatAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        float value;

        value = statement.getFloat(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Sets the specified {@code float} column in the statement.
   * If the value is 0, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setFloat(
    final PreparedStatement statement,
    final int columnIndex,
    final float value) throws SQLException{

        if (value == 0){
      statement.setNull(columnIndex, Types.FLOAT);
    }
    else{
      statement.setFloat(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code float} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setFloat(
    final PreparedStatement statement,
    final int columnIndex,
    final Float value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.FLOAT);
    }
    else{
      statement.setFloat(columnIndex, value.floatValue());
    }

  }

  /**
   * Returns the value of the specified {@code double} column in the result set.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected double getDouble(
    final ResultSet resultSet,
    final int columnIndex,
    final double defaultValue) throws SQLException{
        double value;

        value = resultSet.getDouble(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code double} column in the result set as a
   * {@code Double}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @return The value as a {@code Double}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected Double getBoxedDouble(
    final ResultSet resultSet,
    final int columnIndex) throws SQLException{
        double value;

        value = resultSet.getDouble(columnIndex);
    return (resultSet.wasNull() == true) ? null : Double.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code double} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getDoubleAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        double value;

        value = resultSet.getDouble(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code double} column in the statement.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected double getDouble(
    final CallableStatement statement,
    final int columnIndex,
    final double defaultValue) throws SQLException{
        double value;

        value = statement.getDouble(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code double} column in the statement as a
   * {@code Double}.  If the column is SQL {@code NULL}, then null is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @return The value as a {@code Double}, if the column is not SQL {@code NULL}, otherwise null
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected Double getBoxedDouble(
    final CallableStatement statement,
    final int columnIndex) throws SQLException{
        double value;

        value = statement.getDouble(columnIndex);
    return (statement.wasNull() == true) ? null : Double.valueOf(value);
  }

  /**
   * Returns the value of the specified {@code double} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getDoubleAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        double value;

        value = statement.getDouble(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : String.valueOf(value);
  }

  /**
   * Sets the specified {@code double} column in the statement.
   * If the value is 0, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setDouble(
    final PreparedStatement statement,
    final int columnIndex,
    final double value) throws SQLException{

        if (value == 0){
      statement.setNull(columnIndex, Types.DOUBLE);
    }
    else{
      statement.setDouble(columnIndex, value);
    }

  }

  /**
   * Sets the specified {@code float} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setDouble(
    final PreparedStatement statement,
    final int columnIndex,
    final Double value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.DOUBLE);
    }
    else{
      statement.setDouble(columnIndex, value.doubleValue());
    }

  }

  /**
   * Returns the value of the specified {@code BigDecimal} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getBigDecimalAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        BigDecimal value;

        value = resultSet.getBigDecimal(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value.toString();
  }

  /**
   * Returns the value of the specified {@code BigDecimal} column in the statement as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getBigDecimalAsString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        BigDecimal value;

        value = statement.getBigDecimal(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value.toString();
  }

  /**
   * Sets the specified {@code BigDecimal} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setBigDecimal(
    final PreparedStatement statement,
    final int columnIndex,
    final BigDecimal value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.NUMERIC);
    }
    else{
      statement.setBigDecimal(columnIndex, value);
    }

  }

  /**
   * Returns the value of the specified {@code String} column in the result set.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        String value;

        value = resultSet.getString(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Returns the value of the specified {@code String} column in the statement.
   * If the column is SQL {@code NULL}, then the default value is returned instead.
   * @param statement The statement
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected String getString(
    final CallableStatement statement,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        String value;

        value = statement.getString(columnIndex);
    return (statement.wasNull() == true) ? defaultValue : value;
  }

  /**
   * Sets the specified {@code String} column in the statement.
   * If the value is null, the column is set to SQL {@code NULL}.
   * @param statement The statement
   * @param columnIndex The column index
   * @param value The value to set
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed statement
   */
  protected void setString(
    final PreparedStatement statement,
    final int columnIndex,
    final String value) throws SQLException{

        if (value == null){
      statement.setNull(columnIndex, Types.VARCHAR);
    }
    else{
      statement.setString(columnIndex, value);
    }

  }

  /**
   * Returns the value of the specified {@code clob} column in the result set as a
   * {@code String}.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected String getClobAsString(
    final ResultSet resultSet,
    final int columnIndex,
    final String defaultValue) throws SQLException{
        Clob value;

        value = resultSet.getClob(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value.getSubString(1, (int) value.length());
  }

  /**
   * Returns the value of the specified {@code blob} column in the result set as a
   * {@code byte} array.  If the column is SQL {@code NULL}, then the default value is
   * returned instead.
   * @param resultSet The result set
   * @param columnIndex The column index
   * @param defaultValue The default value
   * @return The value as a {@code String}, if the column is not SQL {@code NULL}, otherwise the default value
   * @throws SQLException if the column index is not valid, if a database access
   * error occurs or this method is called on a closed result set
   */
  protected byte[] getBlobAsBytes(
    final ResultSet resultSet,
    final int columnIndex,
    final byte[] defaultValue) throws SQLException{
        Blob value;

        value = resultSet.getBlob(columnIndex);
    return (resultSet.wasNull() == true) ? defaultValue : value.getBytes(1, (int) value.length());
  }

}
