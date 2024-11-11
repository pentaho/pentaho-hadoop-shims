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


package org.pentaho.hadoop.shim.api.hbase;

import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseService {
  HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig,
                                      LogChannelInterface logChannelInterface )
    throws IOException;

  ColumnFilterFactory getColumnFilterFactory();

  MappingFactory getMappingFactory();

  HBaseValueMetaInterfaceFactory getHBaseValueMetaInterfaceFactory();

  ByteConversionUtil getByteConversionUtil();

  ResultFactory getResultFactory();
}
