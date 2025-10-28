/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.internal.process;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Dzmitry Stsiapanau Date: 01/28/2016 Time: 15:53
 */

@Documented
@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
public @interface RequiredCredentialsToken {

  String CREDENTIALS_REQUIRED_PROPERTY_NAME = "pentaho.mapreduce.required.token.types";

  enum Type {
    HBASE, HIVE, YARN, OOZIE, HDFS
  }

  Type[] value();
}
