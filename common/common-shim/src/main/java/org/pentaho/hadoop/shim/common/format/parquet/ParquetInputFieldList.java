package org.pentaho.hadoop.shim.common.format.parquet;

import org.pentaho.hadoop.shim.api.format.IParquetInputField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParquetInputFieldList implements Iterable<IParquetInputField> {

  private List<IParquetInputField> fields;

  public ParquetInputFieldList( List<IParquetInputField> fields ) {
    this.fields = fields;
  }

  public List<IParquetInputField> getFields() {
    return this.fields;
  }

  public String marshall() {
    StringBuilder o = new StringBuilder( 2048 );
    for ( IParquetInputField f : fields ) {
      o.append( IParquetInputField.marshall( f ) ).append( '\n' );
    }
    return o.toString();
  }

  public static ParquetInputFieldList unmarshall( String str ) {
    List<IParquetInputField> inputFields = new ArrayList<>();
    if ( str.isEmpty() ) {
      return new ParquetInputFieldList( inputFields );
    }
    String[] lines = str.split( "\n" );
    for ( String line : lines ) {
      inputFields.add( ParquetInputField.unmarshallField( line ) );
    }
    return new ParquetInputFieldList( inputFields );
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  public int getFieldsCount() {
    return fields.size();
  }

  @Override
  public Iterator<IParquetInputField> iterator() {
    return fields.iterator();
  }

  @Override
  public String toString() {
    return marshall();
  }
}
