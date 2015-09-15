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

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      if(count % 100 == 0) {
        try {
          Thread.sleep(rand.nextInt(3) * 1);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
      out.emit(Math.random());
    }
  }

  @Override
  public void endWindow()
  {
    for(int counter = 0; counter < numTuples; counter++) {
      if(counter % 100 == 0) {
        try {
          Thread.sleep(rand.nextInt(3) * 500);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
      out.emit(Math.random());
    }
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
