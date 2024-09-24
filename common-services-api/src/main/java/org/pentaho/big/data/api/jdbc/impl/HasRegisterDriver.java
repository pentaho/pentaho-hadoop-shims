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

package org.pentaho.big.data.api.jdbc.impl;

import java.sql.Driver;
import java.sql.SQLException;

/**
 * Created by bryan on 4/29/16.
 */
public interface HasRegisterDriver {
  void registerDriver( Driver driver ) throws SQLException;
}
