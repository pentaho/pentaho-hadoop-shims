/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.internal.mapred;

/**
 * An abstraction for {@link org.apache.hadoop.mapred.TaskCompletionEvent}.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface TaskCompletionEvent {
  /**
   * States of a task
   */
  public static enum Status {
    FAILED,
    KILLED,
    SUCCEEDED,
    OBSOLETE,
    TIPFAILED
  }

  /**
   * Get the state the task is currently in
   *
   * @return Current status (state) of the task
   */
  Status getTaskStatus();

  /**
   * @return the attempt identifier
   */
  Object getTaskAttemptId();

  /**
   * @return the event identifier
   */
  int getEventId();
}
