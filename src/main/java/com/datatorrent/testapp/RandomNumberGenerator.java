/**
 * Put your copyright and license info here.
 */
package com.datatorrent.testapp;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();
  private transient Random rand = new Random();

  private boolean inWindow = false;

  @Override
  public void beginWindow(long windowId)
  {
    inWindow = true;
  }

  @Override
  public void emitTuples()
  {
    if(!inWindow) {
      throw new RuntimeException("emit tuple called within beginWindow");
    }
  }

  @Override
  public void endWindow()
  {
    inWindow = false;
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
