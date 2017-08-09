package org.pentaho.hadoop.shim.api.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Schema description for all Hadoop formats(Parquet/Avro/ORC).
 * 
 * @author Alexander Buloichik
 */
public class SchemaDescription implements Iterable<SchemaDescription.Field> {

  private final List<Field> fields = new ArrayList<>();

  public void addField( Field field ) {
    fields.add( field );
  }

  public String marshall() {
    StringBuilder o = new StringBuilder( 2048 );
    for ( Field f : fields ) {
      o.append( f.marshall() ).append( '\n' );
    }
    return o.toString();
  }

  public static SchemaDescription unmarshall( String str ) {
    SchemaDescription r = new SchemaDescription();
    String[] lines = str.split( "\n" );
    for ( String line : lines ) {
      r.addField( r.unmarshallField( line ) );
    }
    return r;
  }

  public Field unmarshallField( String str ) {
    String[] values = new String[5];
    int prev = 0;
    for ( int i = 0; i < 4; i++ ) {
      int pos = str.indexOf( '|', prev );
      if ( pos < 0 ) {
        throw new RuntimeException( "Wrong field: " + str );
      }
      values[i] = str.substring( prev, pos );
      prev = pos + 1;
    }
    if ( str.indexOf( '|', prev ) >= 0 ) {
      throw new RuntimeException( "Wrong field: " + str );
    }
    values[4] = str.substring( prev );

    Field r = new Field( uc( values[0] ), uc( values[1] ), Integer.parseInt( values[2] ) );
    r.defaultValue = uc( values[3] );
    r.allowNull = Boolean.parseBoolean( values[4] );
    return r;
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }

  public class Field {
    /**
     * Field name in the Hadoop format file.
     */
    public final String formatFieldName;

    /**
     * Field file in the Kettle row.
     */
    public final String pentahoFieldName;

    /**
     * Type id from ValueMetaInterface.
     */
    public final int pentahoValueMetaType;

    public String defaultValue;
    public boolean allowNull;

    public Field( String formatFieldName, String pentahoFieldName, int pentahoValueMetaType ) {
      this.formatFieldName = formatFieldName;
      this.pentahoFieldName = pentahoFieldName;
      this.pentahoValueMetaType = pentahoValueMetaType;
    }

    public String marshall() {
      StringBuilder o = new StringBuilder( 256 );
      o.append( c( formatFieldName ) );
      o.append( '|' );
      o.append( c( pentahoFieldName ) );
      o.append( '|' );
      o.append( Integer.toString( pentahoValueMetaType ) );
      o.append( '|' );
      o.append( c( defaultValue ) );
      o.append( '|' );
      o.append( Boolean.toString( allowNull ) );
      return o.toString();
    }
  }

  public static String c( String s ) {
    if ( s == null ) {
      return "";
    }
    if ( s.contains( "|" ) ) {
      throw new RuntimeException( "Wrong value: " + s );
    }
    return s;
  }

  public static String uc( String s ) {
    if ( s != null && s.isEmpty() ) {
      return null;
    }
    return s;
  }
}
