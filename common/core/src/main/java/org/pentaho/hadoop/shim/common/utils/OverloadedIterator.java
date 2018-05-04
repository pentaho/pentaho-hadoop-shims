/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
