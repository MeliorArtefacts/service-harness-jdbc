/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.exception;
import java.sql.SQLException;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;

/**
 * A standard exception that may be used by data access code to
 * shield the rest of the code from {@code SQLException}.  It is
 * an {@code ApplicationException}, so it is automatically handled
 * wherever {@code ApplicationException} is handled.
 * @author Melior
 * @since 2.2
 * @see ApplicationException
 */
public class DataAccessException extends ApplicationException{

  /**
   * Constructor.
   * @param type The exception type
   * @param message The exception message
   */
  public DataAccessException(
    final ExceptionType type,
    final String message){
        super(type, message);
  }

  /**
   * Constructor.
   * @param type The exception type
   * @param code The exception code
   * @param message The exception message
   */
  public DataAccessException(
    final ExceptionType type,
    final String code,
    final String message){
        super(type, code, message);
  }

  /**
   * Constructor.  The exception type is DATAACCESS_APPLICATION.
   * @param message The exception message
   */
  public DataAccessException(
    final String message){
        super(ExceptionType.DATAACCESS_APPLICATION, message);
  }

  /**
   * Constructor.  The exception type is DATAACCESS_APPLICATION.
   * @param code The exception code
   * @param message The exception message
   */
  public DataAccessException(
    final String code,
    final String message){
        super(ExceptionType.DATAACCESS_APPLICATION, code, message);
  }

  /**
   * Constructor.   If the exception cause is an SQL exception, then the actual type is derived
   * from the exception cause.  Otherwise the exception type is DATAACCESS_APPLICATION.
   * @param message The exception message
   * @param cause The exception cause
   */
  public DataAccessException(
    final String message,
    final Throwable cause){
        super(ExceptionType.DATAACCESS_APPLICATION, message, cause);

        if (cause instanceof SQLException){
            populateException((SQLException) cause);
    }

  }

  /**
   * Constructor.   If the exception cause is an SQL exception, then the actual type is derived
   * from the exception cause.  Otherwise the exception type is DATAACCESS_APPLICATION.
   * @param code The exception code
   * @param message The exception message
   * @param cause The exception cause
   */
  public DataAccessException(
    final String code,
    final String message,
    final Throwable cause){
        super(ExceptionType.DATAACCESS_APPLICATION, code, message, cause);

        if (cause instanceof SQLException){
            populateException((SQLException) cause);
    }

  }

  /**
   * Constructor.   If the exception cause is an SQL exception, then the actual type is derived
   * from the exception cause.  Otherwise the exception type is DATAACCESS_APPLICATION.
   * @param cause The exception cause
   */
  public DataAccessException(
    final Throwable cause){
        super(ExceptionType.DATAACCESS_APPLICATION, cause.getMessage(), cause);

        if (cause instanceof SQLException){
            populateException((SQLException) cause);
    }

  }

  /**
   * Populate exception.
   * @param exception The SQL exception
   */
  private void populateException(
    final SQLException exception){
        type = SQLExceptionMapper.map(exception);

        code = Integer.toString(exception.getErrorCode());
  }

}
