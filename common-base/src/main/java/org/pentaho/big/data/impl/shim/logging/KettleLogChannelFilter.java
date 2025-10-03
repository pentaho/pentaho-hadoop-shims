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


package org.pentaho.big.data.impl.shim.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;

public class KettleLogChannelFilter extends AbstractFilter {
  String logChannelId;

  public KettleLogChannelFilter( String logChannelId ) {
    this.logChannelId = logChannelId;
  }

  @Override
  public Result filter( LogEvent event ) {
    if ( logChannelId.equals( event.getContextData().getValue( "logChannelId" ) ) ) {
      return Result.NEUTRAL;
    }
    return Result.DENY;
  }
}

