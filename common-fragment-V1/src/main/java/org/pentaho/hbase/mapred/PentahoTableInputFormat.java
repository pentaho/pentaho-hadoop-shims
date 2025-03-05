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


package org.pentaho.hbase.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.hadoop.hbase.factory.HBase10ClientFactory;
import org.pentaho.hadoop.shim.api.internal.process.RequiredCredentialsToken;

import java.io.IOException;

/**
 * Extends the mapred TableInputFormat and adds the ability to specify the table to read from via a property (rather
 * than abusing the input path). Also adds more configuration properties (like those int the mapreduce package's
 * implementation).<p>
 * <p/>
 * The following properties can be set in Hitachi Vantara MR job to configure the split:<br><br>
 * <p/>
 * <code> hbase.mapred.inputtable // name of the HBase table to read from hbase.mapred.tablecolumns // space delimited
 * list of columns in ColFam:ColName format (ColName can be ommitted to read all columns from a family)
 * hbase.mapreduce.scan.cachedrows // number of rows for caching that will be passed to scanners
 * hbase.mapreduce.scan.timestamp // timestamp used to filter columns with a specific time stamp
 * hbase.mapreduce.scan.timerange.start // starting timestamp to filter in a given timestamp range
 * hbase.mapreduce.scan.timerange.end // end timestamp to filter in a given timestamp range </code>
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */

@RequiredCredentialsToken( RequiredCredentialsToken.Type.HBASE )
public class PentahoTableInputFormat extends TableInputFormat {

  // Note that the hbase.mapred.tablecolumns property is inherited
  // from TableInputFormat. This property expects a space-delimited list
  // of column names to read in the format "ColumnFamily:ColumnName". The
  // ColumnName may be ommitted in order to read *all* columns from the 
  // specified family

  /**
   * The name of the table to read from
   */
  public static final String INPUT_TABLE = "hbase.mapred.inputtable";

  /**
   * The number of rows (integer) for caching that will be passed to scanners.
   */
  public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";

  /**
   * The timestamp (long) used to filter columns with a specific timestamp.
   */
  public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";

  /**
   * The starting timestamp (long) used to filter columns with a specific range of versions.
   */
  public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";

  /**
   * The ending timestamp (long) used to filter columns with a specific range of versions.
   */
  public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";

  protected final Log PLOG = LogFactory.getLog( PentahoTableInputFormat.class );

  private PentahoTableInputFormat delegate;

  public void configure( JobConf job ) {

    String tableName = job.get( INPUT_TABLE );

    // columns can be colFam:colName or colFam: 
    // the later can be used to set up a scan that 
    String colArg = job.get( COLUMN_LIST );

    if ( !Const.isEmpty( colArg ) ) {
      String[] colNames = colArg.split( " " );
      byte[][] m_cols = new byte[ colNames.length ][];
      for ( int i = 0; i < m_cols.length; i++ ) {
        String colN = colNames[ i ];
        m_cols[ i ] = Bytes.toBytes( colN );
      }
      setInputColumns( m_cols );
    }

    Configuration conf = HBaseConfiguration.create( job );

    HBase10ClientFactory hBase10ClientFactory = null;
    try {
      hBase10ClientFactory = new HBase10ClientFactory( conf );
      delegate = hBase10ClientFactory.getTableInputFormatImpl( this, conf );
    } catch ( IOException e ) {
      PLOG.error( StringUtils.stringifyException( e ) );
    }

    try {
      setHBaseTable( conf, tableName );
    } catch ( Exception e ) {
      PLOG.error( StringUtils.stringifyException( e ) );
    }

    // set our table record reader
    PentahoTableRecordReader rr = createRecordReader( conf );

    String cacheSize = job.get( SCAN_CACHEDROWS );
    if ( !Const.isEmpty( cacheSize ) ) {
      rr.setScanCacheRowSize( Integer.parseInt( cacheSize ) );
    }

    String ts = job.get( SCAN_TIMESTAMP );
    if ( !Const.isEmpty( ts ) ) {
      rr.setTimestamp( Long.parseLong( ts ) );
    }

    String tsStart = job.get( SCAN_TIMERANGE_START );
    String tsEnd = job.get( SCAN_TIMERANGE_END );
    if ( !Const.isEmpty( tsStart ) && !Const.isEmpty( tsEnd ) ) {
      rr.setTimeStampRange( Long.parseLong( tsStart ), Long.parseLong( tsEnd ) );
    }

    setTableRecordReader( rr );
  }

  public void validateInput( JobConf job ) throws IOException {
    // expecting a table name
    String tableName = job.get( INPUT_TABLE );
    if ( Const.isEmpty( tableName ) ) {
      throw new IOException( "expecting one table name" );
    }

    // connected to table?
    if ( !checkHBaseTable() ) {
      throw new IOException( "could not connect to table '"
        + tableName + "'" );
    }

    // expecting at least one column/column family

    String colArg = job.get( COLUMN_LIST );
    if ( colArg == null || colArg.length() == 0 ) {
      throw new IOException( "expecting at least one column/column family" );
    }
  }

  protected void setHBaseTable( Configuration conf, String tableName ) throws IOException {
    delegate.setHBaseTable( conf, tableName );
  }

  protected boolean checkHBaseTable() {
    return delegate.checkHBaseTable();
  }

  protected PentahoTableRecordReader createRecordReader( Configuration conf ) {
    return delegate.createRecordReader( conf );
  }
}
