This repository contains a simple PySpark unit test that works, but
fails to execute with Scalene.

# Prepare

```bash
pip install -r requirements.txt
```

# Execute tests (passes)

```bash
pytest tests
```

# Execute tests with Scalene profiler (fails)

```bash
python3 -m scalene --- -m pytest
```

The error message is

```
E                   py4j.protocol.Py4JJavaError: An error occurred while calling o44.count.
E                   : org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (ip-192-168-1-48.ec2.internal executor driver): org.apache.spark.SparkException: 
E                   Bad data in pyspark.daemon's standard output. Invalid port number:
E                     926298423 (0x37363137)
E                   Python command to execute the daemon was:
E                     python3 -m pyspark.daemon
E                   Check that you don't have any unexpected modules or libraries in
E                   your PYTHONPATH:
E                     /Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/python/lib/pyspark.zip:/Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/python/lib/py4j-0.10.9.5-src.zip:/Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/jars/spark-core_2.12-3.3.0.jar
E                   Also, check if you have a sitecustomize.py module in your python path,
E                   or in your python installation, that is printing to standard output
E                       at org.apache.spark.api.python.PythonWorkerFactory.startDaemon(PythonWorkerFactory.scala:244)
E                       at org.apache.spark.api.python.PythonWorkerFactory.createThroughDaemon(PythonWorkerFactory.scala:134)
E                       at org.apache.spark.api.python.PythonWorkerFactory.create(PythonWorkerFactory.scala:107)
E                       at org.apache.spark.SparkEnv.createPythonWorker(SparkEnv.scala:124)
E                       at org.apache.spark.api.python.BasePythonRunner.compute(PythonRunner.scala:164)
E                       at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:65)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.UnionRDD.compute(UnionRDD.scala:106)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
E                       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
E                       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
E                       at org.apache.spark.scheduler.Task.run(Task.scala:136)
E                       at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
E                       at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
E                       at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
E                       at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
E                       at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
E                       at java.base/java.lang.Thread.run(Thread.java:829)
E                   
E                   Driver stacktrace:
E                       at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2672)
E                       at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2608)
E                       at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2607)
E                       at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
E                       at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
E                       at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
E                       at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2607)
E                       at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1182)
E                       at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1182)
E                       at scala.Option.foreach(Option.scala:407)
E                       at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1182)
E                       at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2860)
E                       at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2802)
E                       at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2791)
E                       at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
E                   Caused by: org.apache.spark.SparkException: 
E                   Bad data in pyspark.daemon's standard output. Invalid port number:
E                     926298423 (0x37363137)
E                   Python command to execute the daemon was:
E                     python3 -m pyspark.daemon
E                   Check that you don't have any unexpected modules or libraries in
E                   your PYTHONPATH:
E                     /Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/python/lib/pyspark.zip:/Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/python/lib/py4j-0.10.9.5-src.zip:/Users/iv/Downloads/pyspark-scalene-pytest/venv/lib/python3.9/site-packages/pyspark/jars/spark-core_2.12-3.3.0.jar
E                   Also, check if you have a sitecustomize.py module in your python path,
E                   or in your python installation, that is printing to standard output
E                       at org.apache.spark.api.python.PythonWorkerFactory.startDaemon(PythonWorkerFactory.scala:244)
E                       at org.apache.spark.api.python.PythonWorkerFactory.createThroughDaemon(PythonWorkerFactory.scala:134)
E                       at org.apache.spark.api.python.PythonWorkerFactory.create(PythonWorkerFactory.scala:107)
E                       at org.apache.spark.SparkEnv.createPythonWorker(SparkEnv.scala:124)
E                       at org.apache.spark.api.python.BasePythonRunner.compute(PythonRunner.scala:164)
E                       at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:65)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.UnionRDD.compute(UnionRDD.scala:106)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
E                       at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
E                       at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
E                       at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
E                       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
E                       at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
E                       at org.apache.spark.scheduler.Task.run(Task.scala:136)
E                       at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
E                       at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
E                       at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
E                       at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
E                       at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
E                       at java.base/java.lang.Thread.run(Thread.java:829)
```
