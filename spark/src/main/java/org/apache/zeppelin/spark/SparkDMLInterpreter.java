package org.apache.zeppelin.spark;

import org.apache.sysml.api.MLContext;
import org.apache.sysml.api.MLOutput;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

/**
 * Created by njindal on 3/2/16.
 */
public class SparkDMLInterpreter extends Interpreter {


  //static final Logger LOGGER = LoggerFactory.getLogger(DMLInterpreter.class);

  static {
    Interpreter.register("dml", "spark", SparkDMLInterpreter.class.getName());
  }

  public SparkDMLInterpreter(Properties property) {
    super(property);
  }


  private SparkInterpreter getSparkInterpreter() {
    LazyOpenInterpreter lazy = null;
    SparkInterpreter spark = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    spark = (SparkInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return spark;
  }

  @Override
  public void open() {
  }

  @Override
  public void close() {
  }

  @Override
  public InterpreterResult interpret(String dmlScriptStr, InterpreterContext context) {
    try {

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      System.setOut(ps);

      MLContext ml = new MLContext(getSparkInterpreter().getSparkContext());
      MLOutput output = ml.executeScript(dmlScriptStr);

      String out = os.toString();

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, out);


    } catch (Exception e) {
//      LOGGER.error("Exception in Markdown while interpret ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              InterpreterUtils.getMostRelevantMessage(e));
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.NONE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
            SparkDMLInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }


}
