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

public class InKeyValueOrdinals extends BaseKeyValueOrdinals {
  public InKeyValueOrdinals( RowMetaInterface rowMeta ) {
    super( rowMeta );
  }

  @Override
  protected final String getKeyName() {
    return "key"; //$NON-NLS-1$
  }

  @Override
  protected final String getValueName() {
    return "value"; //$NON-NLS-1$
  }
}
