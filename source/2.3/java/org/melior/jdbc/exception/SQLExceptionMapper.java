/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.jdbc.exception;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import org.melior.service.exception.ExceptionType;

/**
 * Derives the type of data access exception that has occurred with
 * an {@code SQLException} from the {@code SQLState}, the error code
 * and the sub-class of {@code SQLException}.
 * <P>
 * Where it is determined that the {@code SQLException} is of {@code COMMUNICATION}
 * type or of {@code SYSTEM} type, it is required to close the current connection
 * and to open a new connection in order to restore service.
 * @author Melior
 * @since 2.2
 */
public class SQLExceptionMapper {

    /**
     * Constructor.
     */
    private SQLExceptionMapper() {

        super();
    }

    /**
     * Map the given SQL exception to an application exception type.
     * @param exception The SQL exception
     * @return The application exception type
     */
    public static ExceptionType map(
        final SQLException exception) {

        SQLException ex;
        String sqlState;
        int errorCode;

        ex = exception;
        for (int i = 0; ((i < 10) && (ex != null)); i++, ex = ex.getNextException()) {

            sqlState = ex.getSQLState();
            errorCode = ex.getErrorCode();

            if (startsWith(sqlState, "02") == true) {
                return ExceptionType.NO_DATA;
            }

            else if ((startsWith(sqlState, "08") == true)
                || (in(sqlState, "01002", "66000", "69000", "57P01", "57P02", "57P03", "JZ0C0", "JZ0C1") == true)
                || (in(errorCode, 2399, 500150) == true)
                || (instanceOf(ex, SQLTimeoutException.class, SQLRecoverableException.class, SQLInvalidAuthorizationSpecException.class,
                    SQLNonTransientConnectionException.class, SQLTransientConnectionException.class) == true)) {
                return ExceptionType.DATAACCESS_COMMUNICATION;
            }

            else if ((in(sqlState, "0A000", "60000", "61000") == true)
                || (in(errorCode, 600) == true)
                || (instanceOf(ex, SQLNonTransientException.class, SQLTransactionRollbackException.class) == true)) {
                return ExceptionType.DATAACCESS_SYSTEM;
            }

        }

        return ExceptionType.DATAACCESS_APPLICATION;
    }

    /**
     * Check whether the given {@code string} has the given prefix.
     * @param string The string
     * @param prefix The list of candidate prefixes
     * @return true if the string has the prefix, false otherwise
     */
    private static boolean startsWith(
        final String string,
        final String... list) {

        for (int i = 0; i < list.length; i++) {

            if ((string != null) && (string.startsWith(list[i]) == true)) {
                return true;
            }

        }

        return false;
    }

    /**
     * Check whether the given {@code string} is in the given list.
     * @param string The string
     * @param list The list of candidate strings
     * @return true if the string is in the list, false otherwise
     */
    private static boolean in(
        final String string,
        final String... list) {

        for (int i = 0; i < list.length; i++) {

            if (list[i].equals(string) == true) {
                return true;
            }

        }

        return false;
    }

    /**
     * Check whether the given {@code integer} is in the given list.
     * @param integer The integer
     * @param list The list of candidate integers
     * @return true if the integer is in the list, false otherwise
     */
    private static boolean in(
        final int integer,
        final int... list) {

        for (int i = 0; i < list.length; i++) {

            if (integer == list[i]) {
                return true;
            }

        }

        return false;
    }

    /**
     * Check whether the given exception is an instance of one of the classes in the given list.
     * @param exception The exception
     * @param list The list of candidate classes
     * @return true if the exception is an instance of one of the classes in the list, false otherwise
     */
    private static boolean instanceOf(
        final SQLException exception,
        final Class... list) {

        Class clazz;

        clazz = exception.getClass();

        for (int i = 0; i < list.length; i++) {

            if (clazz == list[i]) {
                return true;
            }

        }

        return false;
    }

}
