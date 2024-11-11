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


package org.pentaho.hadoop.shim.api.internal;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Denotes a {@link org.pentaho.hadoop.shim.spi.PentahoHadoopShim} is required to be provided by a Hadoop Configuration.
 * If a Hadoop Configuration does not supply a shim implementation that is required it is not a valid configuration.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
public @interface Required {

}
