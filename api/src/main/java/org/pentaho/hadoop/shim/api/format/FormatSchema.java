package org.pentaho.hadoop.shim.api.format;


import java.util.List;

//TODO add format related config?
public interface FormatSchema {

  List<FormatField> getFields();

}
