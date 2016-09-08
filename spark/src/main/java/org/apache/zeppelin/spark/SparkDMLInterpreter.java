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
import org.apache.sysml.api.mlcontext.*;
import org.apache.sysml.parser.dml.DmlBaseListener;
import org.apache.sysml.parser.dml.DmlLexer;
import org.apache.sysml.parser.dml.DmlParser;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * SystemML (https://systemml.apache.org/) DML Interpreter
 */
public class SparkDMLInterpreter extends Interpreter {

  public static Logger logger = LoggerFactory.getLogger(SparkDMLInterpreter.class);

  /**
   * Maintains values of all variables between DML cells
   */
  public Map<String, Object> variablesMap = new HashMap<String, Object>();
  //public LocalVariableMap localVariableMap = new LocalVariableMap();

  /**
   * Maintains all the functions defined in the notebook
   */
  public Map<String, String> functionDefs = new HashMap<String, String>();

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
    variablesMap.clear();
    functionDefs.clear();
  }

  @Override
  public void destroy() {
    // Clear variables held between DML cells
    variablesMap.clear();
    functionDefs.clear();
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

    final String prefix = "z";  // Prefix for variables for ZeppelinContext

    try {

      // Capture all output spit out as a result of executing the script
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      System.setOut(ps);

      // DML Var Name -> ZeppelinContextName
      final Map<String, String> zeppelinReadVars = new HashMap<String, String>();
      final Map<String, String> zeppelinWriteVars = new HashMap<String, String>();


      final Set<String> readVars = new HashSet<String>();

      ParseTree tree = getParseTree(dmlScriptStr);

      // All DML Variables that have been read from or written to
      final Set<String> rwVars = parseAllReadWrittenVariables(tree);

      // Collect DML Variables that are LHS in any assignment
      final Set<String> writeVars = parseLHSVariables(tree);

      // Collect all function definitions
      Map<String, String> localFunctionDefs = collectFunctionDefs(dmlScriptStr, tree);
      functionDefs.putAll(localFunctionDefs);

      // Collect the DML variables that will be registered as input and output
      parseZeppelinVars(tree, prefix, zeppelinReadVars, zeppelinWriteVars);

      // Collect all function usages
      Set<String> functionCalls = parseFunctionUsages(tree);

      // Create list of functions to prepend and map of function -> function_body
      Set<String> functionsToAppend = new HashSet<String>(functionCalls);
      functionsToAppend.retainAll(functionDefs.keySet()); // Intersection of called and defined
      functionsToAppend.removeAll(localFunctionDefs.keySet()); // Remove functions defined in cell
      // functionsToAppend contains name of functions that are called in this cell and for which
      // definitions are available.

      // Create map of function name and function bodies that need to be
      // appended to the current cell
      Map<String, String> functionsToAppendMap = new HashMap<String, String>();
      for (String f : functionsToAppend){
        String functionBody = functionDefs.get(f);
        if (functionBody == null)
          throw new Exception("Could not function body for function named " + f);
        functionsToAppendMap.put(f, functionBody);
      }

      // Create the MLContext instance &
      // Get the ZeppelinContext object shared by the "Spark Family" of interpreters
      SparkContext sc = getSparkInterpreter().getSparkContext();
      MLContext ml = new MLContext(sc);
      Script script = new Script();
      ZeppelinContext z = getSparkInterpreter().getZeppelinContext();
      z.setGui(context.getGui());
      z.setInterpreterContext(context);

      // Registers the DML variables as inputs and outputs for ZeppelinContext
      registerVarsForZeppelin(zeppelinReadVars, zeppelinWriteVars, script, z);

      // Register input variables
      registerInputVars(rwVars, script);

      // Also inserts function definitions
      StringBuffer prepend = createPrependString(functionsToAppendMap);

      // Register output variables for all "written" to variables
      registerOutputVars(writeVars, script);


//      statusMessages.append("\nVariables being injected from previous cells : ");
//      for (String v : lvm.keySet()){
//        statusMessages.append(v).append(":").append(lvm.get(v).getDataType()).append(",");
//      }

      //LocalVariableMap lvm = script.getSymbolTable();
      //lvm.putAll(localVariableMap);

      String scriptString = prepend.toString()  + "\n" + dmlScriptStr;
      script.setScriptString(scriptString);
      logger.info(script.getScriptExecutionString());

      // Execute the script
      MLResults output = ml.execute(script);

      // Save the DML Variables that are "write"-en to in the Zeppelin Context Object
      saveToZeppelinContext(zeppelinWriteVars, z, output);

      for (String v : writeVars){
        Object o = output.get(v);
        Object previous = variablesMap.put(v, o);
        logger.info("Inserted value for " + v + " into variablesMap");
        if (previous != null){
          logger.info ("Replaced value of variable " + v);
        }
      }

      //LocalVariableMap lvmAfterRun = script.getSymbolTable();
      //localVariableMap.putAll(lvmAfterRun);
//      Set<String> availableVarSet = lvmAfterRun.keySet();

      // LHS Vars - Saved Vars
      // lhsVars.removeAll(availableVarSet);

//      if (availableVarSet.size() > 0)
//        logger.info( "\n" + "Saved variables : ");
//      for (String s : availableVarSet){
//        logger.info(s + ", ");
//      }
//      if (lhsVars.size() > 0)
//        logger.info("\n" + "Optimized away : ");
//      for (String s : lhsVars){
//        logger.info(s + ", ");
//      }

      String out = os.toString();
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, out);


    } catch (Exception e) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      e.printStackTrace(ps);
      try { baos.close(); } catch (Exception e1) { logger.error(e1.getMessage()); }
      logger.error(baos.toString());
      return new InterpreterResult(InterpreterResult.Code.ERROR,
              InterpreterUtils.getMostRelevantMessage(e));
    }
  }

  /**
   * Registers all output variables
   * @param writeVariables
   * @param script
   * @throws DMLRuntimeException
   */
  private void registerOutputVars(
    Set<String> writeVariables, Script script) throws DMLRuntimeException {
    for (String v : writeVariables) {
      script.out(v);
    }
  }


  /**
   * Create a string with function bodies
   * @param functionsToAppendMap
   * @return
   * @throws DMLRuntimeException
   */
  private StringBuffer createPrependString(
     Map<String, String> functionsToAppendMap) throws DMLRuntimeException {
    StringBuffer prepend = new StringBuffer();
    for (String b : functionsToAppendMap.values()){
      prepend.append("\n").append(b).append("\n");
    }
    return prepend;
  }


  /**
   * Register all input variables
   * @param readVariables
   * @param script
   */
  private void registerInputVars(Set<String> readVariables, Script script) {
    for (String v : readVariables) {
      Object d = this.variablesMap.get(v);
      if (d == null) {
        logger.warn("Could not find value for " + v + " in variables map from previous cell");
        continue;
      } else {
        script.in(v, d);
      }
    }
  }

  /**
   * Return in a set, all the variables that have been written to or read from
   * @param tree
   * @return names of all the DML variables that have written to or read from
   */
  private Set<String> parseAllReadWrittenVariables(ParseTree tree) {
    final Set<String> rwVariables = new HashSet<String>();
    DmlBaseListener readwriteListener = new DmlBaseListener(){

      // If the read/write variables happen inside a function definition, ignore them
      boolean inFunctionDef = false;
      @Override public void enterInternalFunctionDefExpression(
              @NotNull DmlParser.InternalFunctionDefExpressionContext ctx) {
        inFunctionDef = true;
      }

      @Override public void exitInternalFunctionDefExpression(
              @NotNull DmlParser.InternalFunctionDefExpressionContext ctx) {
        inFunctionDef = false;
      }

      @Override public void enterExternalFunctionDefExpression(
              @NotNull DmlParser.ExternalFunctionDefExpressionContext ctx)  {
        inFunctionDef = true;
      }

      @Override public void exitExternalFunctionDefExpression(
              @NotNull DmlParser.ExternalFunctionDefExpressionContext ctx) {
        inFunctionDef = false;
      }


      @Override public void exitSimpleDataIdentifierExpression(
              @NotNull DmlParser.SimpleDataIdentifierExpressionContext ctx) {
        if (!inFunctionDef) {
          rwVariables.add(ctx.ID().getText());
        }
      }

      @Override public void exitIndexedExpression(
              @NotNull DmlParser.IndexedExpressionContext ctx) {
        if (!inFunctionDef) {
          rwVariables.add(ctx.name.getText());
        }
      }

    };

    ParseTreeWalker.DEFAULT.walk(readwriteListener, tree);
    return rwVariables;
  }

  /**
   * Returns all the functions defined in the given tree as a
   * Map from function name to the function definition (including the signature)
   * @param tree
   * @return
   */
  private Map<String, String> collectFunctionDefs(final String dmlScript, ParseTree tree){
    final Map<String, String> functionDefinitions = new HashMap<String, String>();
    DmlBaseListener functionDefListener = new DmlBaseListener() {

      @Override public void exitInternalFunctionDefExpression(
              @NotNull DmlParser.InternalFunctionDefExpressionContext ctx) {
        String name = ctx.name.getText();
        int startIndex = ctx.getStart().getStartIndex();
        int stopIndex = ctx.getStop().getStopIndex();
        String body = dmlScript.substring(startIndex, stopIndex + 1);
        functionDefinitions.put(name, body);
      }

      @Override public void enterExternalFunctionDefExpression(
              @NotNull DmlParser.ExternalFunctionDefExpressionContext ctx) {
        String name = ctx.name.getText();
        int startIndex = ctx.getStart().getStartIndex();
        int stopIndex = ctx.getStop().getStopIndex();
        String body = dmlScript.substring(startIndex, stopIndex + 1);
        functionDefinitions.put(name, body);
      }
    };
    ParseTreeWalker.DEFAULT.walk(functionDefListener, tree);
    return functionDefinitions;
  }

  /**
   * Returns the names of all functions that have been used.
   * @param tree
   * @return
   */
  private Set<String> parseFunctionUsages(ParseTree tree){
    final Set<String> functionUsages = new HashSet<String>();
    DmlBaseListener functionUseListener = new DmlBaseListener() {
      @Override public void exitFunctionCallAssignmentStatement(
              @NotNull DmlParser.FunctionCallAssignmentStatementContext ctx) {
        String functionName = ctx.name.getText();
        functionUsages.add(functionName);
      }

      @Override public void exitFunctionCallMultiAssignmentStatement(
              @NotNull DmlParser.FunctionCallMultiAssignmentStatementContext ctx) {
        String functionName = ctx.name.getText();
        functionUsages.add(functionName);
      }
    };
    ParseTreeWalker.DEFAULT.walk(functionUseListener, tree);
    return functionUsages;
  }

  /**
   * Return in a set, all the variables that have been written to
   * @param tree
   * @return names of all the DML variables that have written to
   */
  private Set<String> parseLHSVariables(ParseTree tree) {
    final Set<String> lhsVariableSet = new HashSet<String>();
    DmlBaseListener lhsListener = new DmlBaseListener() {

      // If the read/write variables happen inside a function definition, ignore them
      boolean inFunctionDef = false;
      @Override public void enterInternalFunctionDefExpression(
              @NotNull DmlParser.InternalFunctionDefExpressionContext ctx) {
        inFunctionDef = true;
      }

      @Override public void exitInternalFunctionDefExpression(
              @NotNull DmlParser.InternalFunctionDefExpressionContext ctx) {
        inFunctionDef = false;
      }

      @Override public void exitFunctionCallAssignmentStatement(
              @NotNull DmlParser.FunctionCallAssignmentStatementContext ctx) {
        if (!inFunctionDef) {
          addDIToVariableSet(ctx.targetList);
        }
      }

      @Override public void exitFunctionCallMultiAssignmentStatement(
              @NotNull DmlParser.FunctionCallMultiAssignmentStatementContext ctx){
        for (DmlParser.DataIdentifierContext di : ctx.dataIdentifier()) {
          if (!inFunctionDef) {
            addDIToVariableSet(di);
          }
        }
      }

      @Override public void exitIfdefAssignmentStatement(
              @NotNull DmlParser.IfdefAssignmentStatementContext ctx){
        if (!inFunctionDef) {
          addDIToVariableSet(ctx.targetList);
        }
      }

      @Override public void exitAssignmentStatement(
              @NotNull DmlParser.AssignmentStatementContext ctx){
        if (!inFunctionDef) {
          addDIToVariableSet(ctx.targetList);
        }
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
    return lhsVariableSet;
  }

  /**
   * Save the DML Variables that are "write"-en to in the {@link ZeppelinContext} instance
   * @param writeVariablesMap
   * @param z
   * @param output
   * @throws DMLRuntimeException
   */
  private void saveToZeppelinContext(
          Map<String, String> writeVariablesMap,
          ZeppelinContext z,
          MLResults output) throws DMLRuntimeException {
    if (writeVariablesMap.size() > 0)
      logger.info("\nDataframes put into ZeppelinContext : ");
    for (Map.Entry<String, String> o : writeVariablesMap.entrySet()) {
      String dmlName = o.getKey();
      String zeppelinContextName = o.getValue();
      DataFrame df = output.getDataFrame(dmlName);
      z.put(zeppelinContextName, df);
      logger.info(zeppelinContextName + ", ");
    }
    logger.info("\n");
  }

  /**
   * Calls {@link Script#in(String, Object)} on the variables that need to be
   * read in from the {@link ZeppelinContext} instance and {@link Script#out(String)}
   * on the variables that need to be written out.
   * @param readVars (in)
   * @param writeVars (in)
   * @param script
   * @param z {@link ZeppelinContext} instance
   * @throws Exception
   */
  private void registerVarsForZeppelin(
          Map<String, String> readVars,
          Map<String, String> writeVars,
          Script script,
          ZeppelinContext z) throws Exception {

    if (readVars.size() > 0) {
      logger.info("\nDataframes made available to DML : ");
    }
    for (Map.Entry<String, String> o : readVars.entrySet()) {
      String dmlName = o.getKey();
      String zeppelinContextName = o.getValue();
      Object zcObject = z.get(zeppelinContextName);
      if (zcObject instanceof DataFrame) {
        script.in(dmlName, zcObject);
        logger.info(dmlName + ", ");
      } else if (zcObject instanceof RDD) {
        throw new Exception("Objects of type RDD cannot be passed through the" +
                " ZeppelinContext object yet!");
      } else if (zcObject instanceof JavaRDD) {
        throw new Exception("Objects of type JavaRDD cannot be passed through the" +
                " ZeppelinContext object yet!");
      } else {
        throw new Exception("Objects of types other than DataFrame cannot be passed through the" +
                " ZeppelinContext object yet!");
      }
    }
    logger.info("\n");

    for (Map.Entry<String, String> o : writeVars.entrySet()) {
      String dmlName = o.getKey();
      script.out(dmlName);
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
  private void parseZeppelinVars(
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
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.NONE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return getSparkInterpreter().getProgress(context);
  }

  @Override
  public Scheduler getScheduler() {
    Interpreter intp = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());
    if (intp != null) {
      return intp.getScheduler();
    } else {
      return SchedulerFactory.singleton().
              createOrGetFIFOScheduler(SparkDMLInterpreter.class.getName() + this.hashCode());
    }
  }

}
