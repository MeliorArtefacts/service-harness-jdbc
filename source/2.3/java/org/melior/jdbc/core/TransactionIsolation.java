package org.melior.jdbc.core;

/**
 * List of transaction isolation levels.
 * @author Melior
 * @since 2.2
 */
public enum TransactionIsolation{
  NONE (0),
  READ_UNCOMMITTED (1),
  READ_COMMITTED (2),
  REPEATABLE_READ (4),
  SERIALIZABLE (8),
  SNAPSHOT (4096);

    private int value;

  /**
   * Constructor.
   * @param value The value
   */
  TransactionIsolation(
    final int value){
        this.value = value;
  }

  /**
   * Get value.
   * @return The value
   */
  public int value(){
    return value;
  }

}
