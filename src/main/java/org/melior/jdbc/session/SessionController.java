/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.jdbc.session;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Interface for implementations that are required to perform additional actions
 * on a connection when the connection is opened by the {@code DataSource}, to
 * prepare the connection for use.  An implementation may retrieve the session
 * data that is associated with the connection at the same time.
 * @author Melior
 * @since 2.2
 */
public interface SessionController
{

	/**
	 * Prepare session for use.
	 * @param connection The database connection
	 * @param dataSource The data source
	 * @return The session data
	 * @throws SQLException when unable to retrieve the session data
	 */
	public SessionData prepareSession(
		final DataSource dataSource,
		final Connection connection) throws SQLException;

}
