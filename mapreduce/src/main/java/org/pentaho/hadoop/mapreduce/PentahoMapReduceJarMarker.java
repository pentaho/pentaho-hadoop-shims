package org.pentaho.hadoop.mapreduce;

// READ THE COMMENTS BELOW BEFORE DELETING THIS CLASS!!!!!

// This class does nothing. The class needs to live in the same jar as org.pentaho.hadoop.mapreduce.PentahoMapRunnable.

// When running the mapreduce step, the shim code, which lives inside Karaf, tries to find the jar
// that contains the PentahoMapRunnable class. It does this by calling Class.forName("org.pentaho.hadoop.mapreduce
// .<classname>).
// We then use the class loader for the class to find the file path to the jar that the class was loaded from.

// Since PentahoMapRunnable has dependencies on jars not contained within karaf, we can't call
// Class.forName("org.pentaho.hadoop.mapreduce.PentahoMapRunnable") from the shim, because the class doesn't load
// properly.
// Instead we call Class.forName("org.pentaho.hadoop.mapreduce.PentahoMapReduceJarMarker") which is this class.
// Since this class has no other dependencies it loads properly. We can then use the class loader to find where the
// jar is located.

// Bottom line is that this class helps the shims find the jar where PentahoMapRunnable lives.

public class PentahoMapReduceJarMarker {
}
