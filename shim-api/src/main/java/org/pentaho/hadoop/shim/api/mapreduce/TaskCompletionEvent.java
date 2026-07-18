/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hadoop.shim.api.mapreduce;

/**
 * Created by bryan on 12/3/15.
 */
public interface TaskCompletionEvent {
  /**
   * Returns the status of the event
   *
   * @return the status of the event
   */
  TaskCompletionEvent.Status getTaskStatus();

  /**
   * Returns the task attempt id
   *
   * @return the task attempt id
   */
  Object getTaskAttemptId();

  /**
   * Returns the event id
   *
   * @return the event id
   */
  int getEventId();

  /**
   * Enumeration of possible status codes
   */
  enum Status {
    FAILED,
    KILLED,
    SUCCEEDED,
    OBSOLETE,
    TIPFAILED;
  }
}
