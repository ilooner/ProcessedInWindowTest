/**
 * Put your copyright and license info here.
 */
package com.datatorrent.testapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(1000000);

    RandomNumberGenerator randomGenerator2 = dag.addOperator("randomGenerator2", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(1000000);

    TestProcessInWindow testProcessInWindow = dag.addOperator("TestProcessInWindow", TestProcessInWindow.class);

    dag.addStream("randomData", randomGenerator.out, testProcessInWindow.input);
    dag.addStream("randomData2", randomGenerator2.out, testProcessInWindow.input2);
  }
}
