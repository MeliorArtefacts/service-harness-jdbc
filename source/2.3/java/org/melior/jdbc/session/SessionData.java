/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.jdbc.session;

/**
 * Captures the session data that is associated with the connection
 * that was opened by the {@code DataSource}.  When enabled, the
 * session data is recorded in the service logs.
 * @author Melior
 * @since 2.2
 */
public class SessionData{
    private String sessionId;

    private long timeDelta;

  /**
   * Constructor.
   */
  public SessionData(){
        super();
  }

  /**
   * Get session identifier.
   * @return The session identifier
   */
  public String getSessionId(){
    return sessionId;
  }

  /**
   * Set session identifier.
   * @param sessionId The session identifier
   */
  public void setSessionId(
    final String sessionId){
        this.sessionId = sessionId;
  }

  /**
   * Get time delta.
   * @return The time delta
   */
  public long getTimeDelta(){
    return timeDelta;
  }

  /**
   * Set time delta.
   * @param timeDelta The time delta
   */
  public void setTimeDelta(
    final long timeDelta){
        this.timeDelta = timeDelta;
  }

}
