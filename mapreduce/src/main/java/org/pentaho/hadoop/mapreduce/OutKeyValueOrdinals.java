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



package org.pentaho.hadoop.mapreduce;

import org.pentaho.di.core.row.RowMetaInterface;

public class OutKeyValueOrdinals extends BaseKeyValueOrdinals {
  public OutKeyValueOrdinals( RowMetaInterface rowMeta ) {
    super( rowMeta );
  }

  @Override
  protected final String getKeyName() {
    return "outKey"; //$NON-NLS-1$
  }

  @Override
  protected final String getValueName() {
    return "outValue"; //$NON-NLS-1$
  }
}
