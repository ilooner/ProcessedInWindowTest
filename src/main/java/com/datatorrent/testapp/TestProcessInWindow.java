/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.testapp;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.IdleTimeHandler;

import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestProcessInWindow implements Operator
{
  private transient Semaphore lock = new Semaphore(0);
  private boolean inWindow = false;

  public final transient DefaultInputPort<Double> input = new DefaultInputPort<Double>() {
    @Override
    public void process(Double t)
    {
      if(!inWindow) {
        throw new RuntimeException("Not In Window");
      }

      try {
        lock.acquire();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }

      try {
        Thread.sleep(1);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }

      lock.release();
    }
  };

  public final transient DefaultInputPort<Double> input2 = new DefaultInputPort<Double>() {
    @Override
    public void process(Double t)
    {
      if(!inWindow) {
        throw new RuntimeException("Not In Window");
      }

      try {
        lock.acquire();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }

      lock.release();
    }
  };

  @Override
  public void beginWindow(long l)
  {
    lock.release();

    inWindow = true;
  }

  @Override
  public void endWindow()
  {
    inWindow = false;

    try {
      lock.acquire();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void setup(OperatorContext cntxt)
  {
  }

  @Override
  public void teardown()
  {
  }

  /*
  @Override
  public void handleIdleTime()
  {
    if (!inWindow) {
      throw new RuntimeException("Not In Window");
    }

    try {
      lock.acquire();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    lock.release();
  }
  */
}
