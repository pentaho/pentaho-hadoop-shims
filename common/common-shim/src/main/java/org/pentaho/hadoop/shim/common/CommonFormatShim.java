package org.pentaho.hadoop.shim.common;


import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.format.FormatSchema;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.OutputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {

  @Override public InputFormat inputFormat() {
    return null;
  }

  @Override public InputFormat inputFormat( FormatSchema formatSchema ) {
    return null;
  }

  @Override public OutputFormat outputFormat() {
    return null;
  }

  @Override public OutputFormat outputFormat( FormatSchema formatSchema ) {
    return null;
  }

  @Override public ShimVersion getVersion() {
    return null;
  }
}
