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

package org.pentaho.hadoop.shim.common.utils;

import java.util.Iterator;

/**
 * User: Dzmitry Stsiapanau Date: 11/16/2015 Time: 14:12
 */
public interface OverloadedIterator<E> extends Iterator<E> {

  /**
   * Returns the next element in the iteration.
   *
   * @param args Types which are used in non-default constructor (Class<?>) followed by their values. For example: (
   *             String.class, "someString", Integer.class, 0 )
   * @return the next element in the iteration
   * @throws java.util.NoSuchElementException if the iteration has no more elements
   */
  E next( Object... args );

}
