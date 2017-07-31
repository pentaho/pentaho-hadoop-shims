package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.mapreduce.InputSplit;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;

/**
 * Created by Vasilina_Terehova on 8/1/2017.
 */
public class PentahoInputSplitImpl implements PentahoInputSplit {
  InputSplit inputSplit;

    public PentahoInputSplitImpl(InputSplit inputSplit) {
        this.inputSplit = inputSplit;
    }

    InputSplit getInputSplit() {
        return inputSplit;
    }
}
