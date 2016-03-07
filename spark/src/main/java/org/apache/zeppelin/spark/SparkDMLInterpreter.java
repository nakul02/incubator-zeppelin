package org.apache.zeppelin.spark;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.sysml.api.MLContext;
import org.apache.sysml.api.MLOutput;
import org.apache.sysml.parser.dml.DmlBaseListener;
import org.apache.sysml.parser.dml.DmlLexer;
import org.apache.sysml.parser.dml.DmlParser;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by njindal on 3/2/16.
 */
public class SparkDMLInterpreter extends Interpreter {


  public static Logger logger = LoggerFactory.getLogger(SparkDMLInterpreter.class);

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

    // In a DML statement
    // Z = read($zY)
    // An object named "Y" is a data frame that was put into the ZeppelinContext object previously
    // (from a Spark Cell)
    // In a DML statement
    // write(W, $zWout)
    // W is a Java object that is put into the ZeppelinContext object with name "Wout"

    final String prefix = "z";  // Prefix
    StringBuffer statusMessages = new StringBuffer();

    try {

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      System.setOut(ps);

      // DML Var Name -> ZeppelinContextName
      final HashMap<String, String> readVariablesMap = new HashMap<String, String>();
      final HashMap<String, String> writeVariablesMap = new HashMap<String, String>();

      // Collect the DML variables that will be registered as input and output

      InputStream stream =
              new ByteArrayInputStream(dmlScriptStr.getBytes(StandardCharsets.UTF_8));
      ANTLRInputStream in = new ANTLRInputStream(stream);
      DmlLexer lexer = new DmlLexer(in);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      DmlParser antlr4Parser = new DmlParser(tokens);
      antlr4Parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      antlr4Parser.removeErrorListeners();
      antlr4Parser.setErrorHandler(new BailErrorStrategy());
      ParseTree tree = antlr4Parser.programroot();
      ParseTreeWalker walker = new ParseTreeWalker();

      DmlBaseListener readWriteStatementListener = new DmlBaseListener() {
        @Override
        public void exitFunctionCallAssignmentStatement(
                @NotNull DmlParser.FunctionCallAssignmentStatementContext ctx) {
          String functionName = ctx.name.getText();
          if (functionName.equals("read")) {

            if (ctx.targetList != null &&
                    ctx.targetList instanceof DmlParser.SimpleDataIdentifierExpressionContext) {
              String lhs = ctx.targetList.getText();
              // Dig down into the parameter value
              assert ctx.paramExprs.size() == 1;
              DmlParser.ExpressionContext exprCtx = ctx.paramExprs.get(0).paramVal;
              if (exprCtx instanceof DmlParser.DataIdExpressionContext) {
                DmlParser.DataIdentifierContext dataid =
                        ((DmlParser.DataIdExpressionContext) exprCtx).dataIdentifier();
                if (dataid instanceof DmlParser.CommandlineParamExpressionContext) {
                  String commandLineParam = dataid.getText();
                  String prefixStr = "$" + prefix;
                  if (commandLineParam.startsWith(prefixStr)) {
                    String zeppelinContextObjName =
                            commandLineParam.substring(prefixStr.length());
                    readVariablesMap.put(lhs, zeppelinContextObjName);
                  }
                }
              }
            }

          } else if (functionName.equals("write")) {
            assert ctx.paramExprs.size() == 2;
            DmlParser.ExpressionContext varCtx = ctx.paramExprs.get(0).paramVal;
            DmlParser.ExpressionContext commandLineParamCtx = ctx.paramExprs.get(1).paramVal;
            if (varCtx instanceof DmlParser.DataIdExpressionContext &&
                    commandLineParamCtx instanceof DmlParser.DataIdExpressionContext) {
              DmlParser.DataIdentifierContext varDataId =
                      ((DmlParser.DataIdExpressionContext) varCtx).dataIdentifier();
              DmlParser.DataIdentifierContext commandLineDataId =
                      ((DmlParser.DataIdExpressionContext) commandLineParamCtx).dataIdentifier();
              if (varDataId instanceof DmlParser.SimpleDataIdentifierExpressionContext) {
                String varName = varDataId.getText();

                if (commandLineDataId instanceof DmlParser.CommandlineParamExpressionContext) {
                  String commandLineParam = commandLineDataId.getText();
                  String prefixStr = "$" + prefix;
                  if (commandLineParam.startsWith(prefixStr)) {
                    String zeppelinContextObjName =
                            commandLineParam.substring(prefixStr.length());
                    writeVariablesMap.put(varName, zeppelinContextObjName);
                  }
                }
              }
            }
          }
        }
      };

      walker.walk(readWriteStatementListener, tree);

//      logger.error("readVariablesMap size " + readVariablesMap.size());
//      for (Map.Entry<String, String> o : readVariablesMap.entrySet()){
//        logger.error(o.getKey() +  "\t" + o.getValue());
//      }
//      logger.error("writeVariablesMap size " + writeVariablesMap.size());
//      for (Map.Entry<String, String> o : writeVariablesMap.entrySet()){
//        logger.error(o.getKey() +  "\t" + o.getValue());
//      }



      SparkContext sc = getSparkInterpreter().getSparkContext();
      SQLContext sqlContext = getSparkInterpreter().getSQLContext();

      MLContext ml = new MLContext(sc);
      ZeppelinContext z = getSparkInterpreter().getZeppelinContext();
      z.setGui(context.getGui());
      z.setInterpreterContext(context);

      for (Map.Entry<String, String> o : readVariablesMap.entrySet()) {
        String dmlName = o.getKey();
        String zeppelinContextName = o.getValue();
        Object zcObject = z.get(zeppelinContextName);
        if (zcObject instanceof DataFrame) {
          ml.registerInput(dmlName, (DataFrame) zcObject);
          statusMessages.append("Dataframe " + dmlName + " made available to DML\n");
        } else if (zcObject instanceof RDD) {
          throw new Exception("Objects of type RDD cannot be passed through the" +
                  " ZeppelinContext object yet!");
          // ml.register
        } else if (zcObject instanceof JavaRDD) {
          // ml.register
          throw new Exception("Objects of type JavaRDD cannot be passed through the" +
                  " ZeppelinContext object yet!");
        } else {
          throw new Exception("Objects of types other than DataFrame cannot be passed through the" +
                  " ZeppelinContext object yet!");
        }
      }

//      out += "\t\t" + z.get("C");

      for (Map.Entry<String, String> o : writeVariablesMap.entrySet()) {
        String dmlName = o.getKey();
        ml.registerOutput(dmlName);
      }

      MLOutput output = ml.executeScript(dmlScriptStr);

      for (Map.Entry<String, String> o : writeVariablesMap.entrySet()) {
        String dmlName = o.getKey();
        String zeppelinContextName = o.getValue();
        DataFrame df = output.getDF(sqlContext, dmlName);
        z.put(zeppelinContextName, df);
        statusMessages.append("Dataframe " + zeppelinContextName + " put into zeppelincontext\n");
      }

      String out = os.toString();

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, out
              + "\n"
              + statusMessages.toString());


    } catch (Exception e) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      e.printStackTrace(ps);
      try { baos.close(); } catch (Exception e1) { logger.error(e1.getMessage()); }
      logger.error(baos.toString());
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              InterpreterUtils.getMostRelevantMessage(e)
                      + "\n"
                      + statusMessages.toString());
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
