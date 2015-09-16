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

import com.datatorrent.common.util.NameableThreadFactory;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestProcessInWindow implements Operator
{
  private boolean inWindow = false;

  protected transient ExecutorService processingThread;
  private transient Thread mainThread;
  private final transient Semaphore lock = new Semaphore(0);

  public final transient DefaultInputPort<Double> input = new DefaultInputPort<Double>() {
    @Override
    public void process(Double t)
    {
      if(!inWindow) {
        throw new RuntimeException("Not In Window");
      }
    }
  };

  public final transient DefaultInputPort<Double> input2 = new DefaultInputPort<Double>() {
    @Override
    public void process(Double t)
    {
      if(!inWindow) {
        throw new RuntimeException("Not In Window");
      }
    }
  };

  @Override
  public void beginWindow(long l)
  {
      inWindow = true;

      lock.release();
  }

  @Override
  public void endWindow()
  {
    lock.acquireUninterruptibly();
    inWindow = false;
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Logger.getLogger(TestProcessInWindow.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void setup(OperatorContext cntxt)
  {
    processingThread = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory("Query Executor Thread"));
    processingThread.submit(new TestRunnable(Thread.currentThread()));
  }

  public class TestRunnable implements Runnable
  {
    private Thread mainThread;

    public TestRunnable(Thread mainThread)
    {
      this.mainThread = mainThread;
    }

    @Override
    public void run()
    {
      int count = 0;

      while (true) {

        if (count % 10000000 == 0) {
          LOG.info("{}", count);
        }

        lock.acquireUninterruptibly();

        for(int counter = 0; counter < 100000; counter++) {
          count++;
        }

        lock.release();

        if(System.currentTimeMillis() == -1L) {
          mainThread.interrupt();
        }

        count++;
        Math.abs(count);
      }
    }
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

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestProcessInWindow.class);
}
