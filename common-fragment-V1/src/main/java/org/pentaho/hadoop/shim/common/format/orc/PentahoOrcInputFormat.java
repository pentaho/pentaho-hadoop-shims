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

package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.InputStream;
import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcInputFormat extends HadoopFormatBase implements IPentahoOrcInputFormat {

  protected static final String NOT_NULL_MSG = "filename and inputfields must not be null";
  protected String fileName;
  protected List<? extends IOrcInputField> inputFields;

  protected Configuration conf;

  public PentahoOrcInputFormat( NamedCluster namedCluster ) {
    if ( namedCluster == null ) {
      conf = new Configuration();
    } else {
      conf = inClassloader( () -> {
        Configuration confProxy = new ConfigurationProxy();
        confProxy.addResource( "hive-site.xml" );
        BiConsumer<InputStream, String> consumer = ( is, filename ) -> confProxy.addResource( is, filename );
        ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
        return confProxy;
      } );
    }
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) {
    requireNonNull( fileName, NOT_NULL_MSG );
    requireNonNull( inputFields, NOT_NULL_MSG );
    return inClassloader( () -> new PentahoOrcRecordReader( fileName, conf, inputFields ) );
  }

  @Override
  public List<IOrcInputField> readSchema() {
    return inClassloader( () -> readSchema(
      PentahoOrcRecordReader.getReader(
        requireNonNull( fileName, NOT_NULL_MSG ), conf ) ) );
  }

  protected List<IOrcInputField> readSchema( Reader orcReader ) {
    OrcSchemaConverter orcSchemaConverter = new OrcSchemaConverter();
    List<IOrcInputField> orcInputFields = orcSchemaConverter.buildInputFields( readTypeDescription( orcReader ) );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( orcReader );
    orcMetaDataReader.read( orcInputFields );
    return orcInputFields;
  }

  protected TypeDescription readTypeDescription( Reader orcReader ) {
    return orcReader.getSchema();
  }

  /**
   * Set schema from user's metadata
   * <p>
   * This schema will be used instead of schema from {@link #fileName} since we allow user to override pentaho filed
   * name
   */
  @Override
  public void setSchema( List<IOrcInputField> inputFields ) {
    this.inputFields = inputFields;
  }

  @Override
  public void setInputFile( String fileName ) {
    this.fileName = S3NCredentialUtils.scrubFilePathIfNecessary( fileName );
  }


}
