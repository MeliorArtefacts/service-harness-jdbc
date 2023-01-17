/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.jdbc.util;

/**
 * Captures the difference in system time between the service and the
 * database that the {@code DataSource} connects to.
 * @author Melior
 * @since 2.2
 */
public class TimeDelta {

    private long delta;

    /**
     * Constructor.
     */
    public TimeDelta() {

        super();

        delta = 0;
    }

    /**
     * Get time delta.
     * @return The time delta
     */
    public long getDelta() {
        return delta;
    }

    /**
     * Update time delta.
     * @param delta The time delta
     * @return The new time delta
     */
    public synchronized long setDelta(
        final long delta) {

        this.delta = (this.delta == 0) ? delta : (this.delta + delta) / 2;

        return this.delta;
    }

}
