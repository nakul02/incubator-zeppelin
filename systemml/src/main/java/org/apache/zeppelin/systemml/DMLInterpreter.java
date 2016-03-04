package org.apache.zeppelin.systemml;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.AParserWrapper;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DMLTranslator;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.utils.Statistics;
import org.apache.sysml.yarn.DMLAppMasterUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by njindal on 2/26/16.
 */
public class DMLInterpreter extends Interpreter {

  //static final Logger LOGGER = LoggerFactory.getLogger(DMLInterpreter.class);

  static {
    Interpreter.register("dml", DMLInterpreter.class.getName());
  }

  public DMLInterpreter(Properties property) {
    super(property);
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

      DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.SINGLE_NODE;
      DMLConfig conf = new DMLConfig();
      ConfigurationManager.setConfig(conf);

      //Step 2: set local/remote memory if requested (for compile in AM context)
      if (conf.getBooleanValue(DMLConfig.YARN_APPMASTER)) {
        DMLAppMasterUtils.setupConfigRemoteMaxMemory(conf);
      }

      //Step 3: parse dml script
      AParserWrapper parser = AParserWrapper.createParser(false);
      DMLProgram prog = parser.parse("test", dmlScriptStr, new HashMap<String, String>());


      //Step 4: construct HOP DAGs (incl LVA and validate)
      DMLTranslator dmlt = new DMLTranslator(prog);
      dmlt.liveVariableAnalysis(prog);
      dmlt.validateParseTree(prog);
      dmlt.constructHops(prog);


      //Step 5: rewrite HOP DAGs (incl IPA and memory estimates)
      dmlt.rewriteHopsDAG(prog);

      //Step 6: construct lops (incl exec type and op selection)
      dmlt.constructLops(prog);

      //Step 7: generate runtime program
      Program rtprog = prog.getRuntimeProgram(conf);

      //Step 10: execute runtime program
      Statistics.startRunTimer();
      ExecutionContext ec = null;

      //run execute (w/ exception handling to ensure proper shutdown)
      ec = ExecutionContextFactory.createContext(rtprog);
      rtprog.execute(ec);

      String out = os.toString();

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, out);


    } catch (Exception e) {
//      LOGGER.error("Exception in Markdown while interpret ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              InterpreterUtils.getMostRelevantMessage(e));
    }
  }

  @Override
  public void cancel(InterpreterContext context) {}

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
            DMLInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

}
