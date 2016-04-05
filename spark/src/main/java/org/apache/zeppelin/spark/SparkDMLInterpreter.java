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
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.dml.DmlBaseListener;
import org.apache.sysml.parser.dml.DmlLexer;
import org.apache.sysml.parser.dml.DmlParser;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.tools.ant.taskdefs.Local;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by njindal on 3/2/16.
 */
public class SparkDMLInterpreter extends Interpreter {

  public static Logger logger = LoggerFactory.getLogger(SparkDMLInterpreter.class);

  /**
   * Maintains values of all variables between DML cells
   */
  public static LocalVariableMap localVariableMap = new LocalVariableMap();

  static {
    Interpreter.register("dml", "spark", SparkDMLInterpreter.class.getName());
  }

  public SparkDMLInterpreter(Properties property) {
    super(property);
  }

  /**
   * Function copied over from {@link SparkSqlInterpreter#getSparkInterpreter()}
   */
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
    // Spurious call to invoke SparkInterpreter.createSparkContext()
    getSparkInterpreter().getSparkContext();
  }

  @Override
  public void close() {
    // Clear variables held between DML cells
    //localVariableMap.removeAll();
  }

  @Override
  public void destroy() {
    // Clear variables held between DML cells
    //localVariableMap.removeAll();
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

      // Capture all output spit out as a result of executing the script
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      System.setOut(ps);

      // DML Var Name -> ZeppelinContextName
      final Map<String, String> readVariablesMap = new HashMap<String, String>();
      final Map<String, String> writeVariablesMap = new HashMap<String, String>();
      final Set<String> lhsVariableSet = new HashSet<String>();

      ParseTree tree = getParseTree(dmlScriptStr);

      // Collect DML Variables that are LHS in any assignment
      parseLHSVariables(tree, lhsVariableSet);

      // Collect the DML variables that will be registered as input and output
      parseReadWriteVariables(tree, prefix, readVariablesMap, writeVariablesMap);

      // Create the MLContext instance &
      // Get the ZeppelinContext object shared by the "Spark Family" of interpreters
      SparkContext sc = getSparkInterpreter().getSparkContext();
      MLContext ml = new MLContext(sc);
      ZeppelinContext z = getSparkInterpreter().getZeppelinContext();
      z.setGui(context.getGui());
      z.setInterpreterContext(context);

      // Registers the DML variables as inputs and outputs
      registerInputOutputVariables(statusMessages, readVariablesMap, writeVariablesMap, ml, z);

      // Move all variables in the localVariableMap from previous cell to this one
      LocalVariableMap lvm = ml.getLocalVariablesMap();
      lvm.putAll(localVariableMap);

      statusMessages.append("\nVariables being injected from previous cells : ");
      for (String v : lvm.keySet()){
        statusMessages.append(v).append(":").append(lvm.get(v).getDataType());
      }

      // Execute the script
      MLOutput output = ml.executeScript(dmlScriptStr);

      // Save the DML Variables that are "write"-en to in the Zeppelin Context Object
      saveToZeppelinContext(statusMessages, writeVariablesMap, z, output);

      LocalVariableMap lvmAfterRun = ml.getLocalVariablesMap();
      localVariableMap.putAll(lvmAfterRun);
      Set<String> availableVarSet = lvmAfterRun.keySet();

      // LHS Vars - Saved Vars
      lhsVariableSet.removeAll(availableVarSet);

      if (availableVarSet.size() > 0)
        statusMessages.append( "\n" + "Saved variables : ");
      for (String s : availableVarSet){
        statusMessages.append(s + ", ");
      }
      if (lhsVariableSet.size() > 0)
        statusMessages.append("\n" + "Optimized away : ");
      for (String s : lhsVariableSet){
        statusMessages.append(s + ", ");
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

  private void parseLHSVariables(ParseTree tree, final Set<String> lhsVariableSet) {
    DmlBaseListener lhsListener = new DmlBaseListener() {

      @Override public void exitFunctionCallAssignmentStatement(
              @NotNull DmlParser.FunctionCallAssignmentStatementContext ctx) {
        addDIToVariableSet(ctx.targetList);
      }

      @Override public void exitFunctionCallMultiAssignmentStatement(
              @NotNull DmlParser.FunctionCallMultiAssignmentStatementContext ctx){
        for (DmlParser.DataIdentifierContext di : ctx.dataIdentifier()) {
          addDIToVariableSet(di);
        }
      }

      @Override public void exitIfdefAssignmentStatement(
              @NotNull DmlParser.IfdefAssignmentStatementContext ctx){
        addDIToVariableSet(ctx.targetList);
      }

      @Override public void exitAssignmentStatement(
              @NotNull DmlParser.AssignmentStatementContext ctx){
        addDIToVariableSet(ctx.targetList);

      }
      private void addDIToVariableSet(DmlParser.DataIdentifierContext di) {
        if (di instanceof DmlParser.IndexedExpressionContext){
          lhsVariableSet.add(((DmlParser.IndexedExpressionContext) di).name.getText());
        } else if (di instanceof DmlParser.SimpleDataIdentifierExpressionContext) {
          lhsVariableSet.add(((DmlParser.SimpleDataIdentifierExpressionContext) di).ID().getText());
        }
      }
    };

    ParseTreeWalker.DEFAULT.walk(lhsListener, tree);
  }

  /**
   * Save the DML Variables that are "write"-en to in the {@link ZeppelinContext} instance
   * @param statusMessages
   * @param writeVariablesMap
   * @param z
   * @param output
   * @throws DMLRuntimeException
   */
  private void saveToZeppelinContext(
          StringBuffer statusMessages,
          Map<String, String> writeVariablesMap,
          ZeppelinContext z,
          MLOutput output) throws DMLRuntimeException {
    SQLContext sqlContext = getSparkInterpreter().getSQLContext();
    if (writeVariablesMap.size() > 0)
      statusMessages.append("\nDataframes put into ZeppelinContext : ");
    for (Map.Entry<String, String> o : writeVariablesMap.entrySet()) {
      String dmlName = o.getKey();
      String zeppelinContextName = o.getValue();
      DataFrame df = output.getDF(sqlContext, dmlName);
      z.put(zeppelinContextName, df);
      statusMessages.append(zeppelinContextName + ", ");
    }
    statusMessages.append("\n");
  }

  /**
   * Calls {@link MLContext#registerInput(String, DataFrame)} on the variables that need to be
   * read in from the {@link ZeppelinContext} instance and {@link MLContext#registerOutput(String)}
   * on the variables that need to be written out.
   * @param statusMessages (out)
   * @param readVariablesMap (in)
   * @param writeVariablesMap (in)
   * @param ml
   * @param z
   * @throws Exception
   */
  private void registerInputOutputVariables(
          StringBuffer statusMessages,
          Map<String, String> readVariablesMap,
          Map<String, String> writeVariablesMap,
          MLContext ml,
          ZeppelinContext z) throws Exception {

    if (readVariablesMap.size() > 0)
      statusMessages.append("\nDataframes made available to DML : ");
    for (Map.Entry<String, String> o : readVariablesMap.entrySet()) {
      String dmlName = o.getKey();
      String zeppelinContextName = o.getValue();
      Object zcObject = z.get(zeppelinContextName);
      if (zcObject instanceof DataFrame) {
        ml.registerInput(dmlName, (DataFrame) zcObject);
        statusMessages.append(dmlName + ", ");
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
    statusMessages.append("\n");

    for (Map.Entry<String, String> o : writeVariablesMap.entrySet()) {
      String dmlName = o.getKey();
      ml.registerOutput(dmlName);
    }
  }

  /**
   * Reads an input DML script and collects the input and output variables.
   * The input variables need to read in and the output variables need to be saved to the
   * {@link ZeppelinContext} instance.
   * A variables is read in from the {@link ZeppelinContext} instance if it appears in a
   * 'read' statement with a ('$' + prefix). A variable is written out to the instance if it appears
   * in a 'write' statement with a ('$' + prefix).
   * <br/>
   * <pre>
   * For example
   *    X = read($zXDF)   // <- reads data frame named "XDF" from the Zeppelin Context Object
   *    Y = ...           // <- computation
   *    write (Y, $zYDF)  // <- writes data from Y to a data frame named "YDF"
   * </pre>
   * @param tree (in)
   * @param prefix (in)
   * @param readVariablesMap (out)
   * @param writeVariablesMap (out)
   * @throws IOException
   */
  private void parseReadWriteVariables(
          ParseTree tree,
          final String prefix,
          final Map<String, String> readVariablesMap,
          final Map<String, String> writeVariablesMap) throws IOException {

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

    ParseTreeWalker.DEFAULT.walk(readWriteStatementListener, tree);
  }

  private ParseTree getParseTree(String dmlScriptStr) throws IOException {
    InputStream stream =
            new ByteArrayInputStream(dmlScriptStr.getBytes(StandardCharsets.UTF_8));
    ANTLRInputStream in = new ANTLRInputStream(stream);
    DmlLexer lexer = new DmlLexer(in);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    DmlParser antlr4Parser = new DmlParser(tokens);
    antlr4Parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
    antlr4Parser.removeErrorListeners();
    antlr4Parser.setErrorHandler(new BailErrorStrategy());
    return antlr4Parser.programroot();
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
