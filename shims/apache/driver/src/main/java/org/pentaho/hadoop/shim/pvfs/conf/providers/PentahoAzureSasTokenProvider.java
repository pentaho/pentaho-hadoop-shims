package org.pentaho.hadoop.shim.pvfs.conf.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class PentahoAzureSasTokenProvider implements SASTokenProvider {

  private String sasToken;

  @Override
  public void initialize( Configuration configuration, String s ) {
    sasToken = configuration.get( "fs.azure.sas.token" );
  }

  @Override
  public String getSASToken( String s, String s1, String s2, String s3 ) {
    //We are not generating a SAS token but using a configured SAS Token.
    return sasToken;
  }
}
