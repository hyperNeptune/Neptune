/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.parser.expression.BinaryExpression;
import cn.edu.thssdb.parser.expression.ColumnRefExpression;
import cn.edu.thssdb.parser.expression.ConstantExpression;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.parser.tableBinder.JoinTableBinder;
import cn.edu.thssdb.parser.tableBinder.RegularTableBinder;
import cn.edu.thssdb.parser.tableBinder.TableBinder;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLLexer;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Pair;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// our binder is a visitor that walks the parse tree and builds a statement tree
// planner walks the statement tree and turn SQL statements into actual operations
// and then the executor executes the operations
public class Binder extends SQLBaseVisitor<Statement> implements Iterable<Statement> {
  Catalog catalog;
  Statement[] statements;

  public Binder(Catalog catalog) {
    this.catalog = catalog;
  }

  private ParseTree parse(String sql) {
    CharStream charStream1 = CharStreams.fromString(sql);

    SQLLexer lexer1 = new SQLLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    SQLParser parser1 = new SQLParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);

    ParseTree tree;
    try {
      // STAGE 1: try with simpler/faster SLL(*)
      tree = parser1.sqlStmtList();
      // if we get here, there was no syntax error and SLL(*) was enough;
      // there is no need to try full LL(*)
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);

      SQLLexer lexer2 = new SQLLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      SQLParser parser2 = new SQLParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = parser2.sqlStmtList();
      // if we get here, it's LL not SLL
    }
    return tree;
  }

  public void parseAndBind(String sql) {
    ParseTree tree = parse(sql);
    visit(tree);
  }

  @Override
  public Iterator<Statement> iterator() {
    if (statements == null) {
      throw new RuntimeException("this binder has no statements at all!");
    }
    return new StatementIterator();
  }

  // Iterator over statements
  class StatementIterator implements java.util.Iterator<Statement> {
    int index = 0;

    @Override
    public boolean hasNext() {
      return index < statements.length;
    }

    @Override
    public Statement next() {
      return statements[index++];
    }
  }

  @Override
  public Statement visitSqlStmtList(SQLParser.SqlStmtListContext ctx) {
    statements = new Statement[ctx.sqlStmt().size()];
    int i = 0;
    for (SQLParser.SqlStmtContext sqlStmtContext : ctx.sqlStmt()) {
      statements[i++] = visitSqlStmt(sqlStmtContext);
    }
    return null;
  }

  private Column bindColumnDef(SQLParser.ColumnDefContext ctx) {
    String columnName = ctx.columnName().getText();
    String typeName = ctx.typeName().getText().toLowerCase();
    // special for string
    Type type;
    if (typeName.startsWith("string") && Type.TYPEDICT.get(typeName) == null) {
      type =
          StringType.getVarCharType(Integer.parseInt(ctx.typeName().INTEGER_LITERAL().getText()));
      Type.TYPEDICT.put(typeName, type);
    } else {
      type = Type.TYPEDICT.get(typeName);
    }
    byte nullable = 1;
    byte primary = 0;
    byte maxlenth = (byte) type.getTypeSize();
    if (ctx.columnConstraint() != null) {
      for (SQLParser.ColumnConstraintContext columnConstraintContext : ctx.columnConstraint()) {
        if (columnConstraintContext.K_PRIMARY() != null) {
          primary = 1;
        } else if (columnConstraintContext.K_NOT() != null) {
          nullable = 0;
        } else if (columnConstraintContext.K_NULL() != null) {
          nullable = 0;
        }
      }
    }
    return new Column(columnName, type, primary, nullable, maxlenth, 0);
  }

  @Override
  public Statement visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    // bind all columns
    String tableName = ctx.tableName().getText();
    Column[] columns = new Column[ctx.columnDef().size()];
    int i = 0;
    for (SQLParser.ColumnDefContext columnDefContext : ctx.columnDef()) {
      columns[i++] = bindColumnDef(columnDefContext);
    }
    // table constraint
    if (ctx.tableConstraint() != null) {
      SQLParser.TableConstraintContext tableConstraintContext = ctx.tableConstraint();
      if (tableConstraintContext.K_PRIMARY() != null) {
        String primaryKeyName = tableConstraintContext.columnName().get(0).getText();
        for (Column column : columns) {
          if (column.getName().equals(primaryKeyName)) {
            column.setPrimary((byte) 1);
          }
        }
      }
    }
    // check primary key, should only have one primary key
    int primaryCount = 0;
    for (Column column : columns) {
      if (column.getPrimary() == 1) {
        primaryCount++;
      }
    }
    if (primaryCount > 1) {
      throw new RuntimeException("primary key should only have one");
    }
    return new CreateTbStatement(tableName, columns);
  }

  // show table xx
  @Override
  public Statement visitShowMetaStmt(SQLParser.ShowMetaStmtContext ctx) {
    return new ShowTbStatement(catalog.getTableInfo(ctx.tableName().getText()));
  }

  /*
   *  methods for parsing expressions
   * */

  private Expression visitConstants(SQLParser.LiteralValueContext ctx) {
    // parse use the biggest type, and exception will be thrown if overflow
    // when casting to smaller type
    if (ctx.FLOAT_LITERAL() != null) {
      return new ConstantExpression(
          new DoubleValue(Double.parseDouble(ctx.FLOAT_LITERAL().getText())));
    } else if (ctx.INTEGER_LITERAL() != null) {
      return new ConstantExpression(new LongValue(Long.parseLong(ctx.INTEGER_LITERAL().getText())));
    } else if (ctx.STRING_LITERAL() != null) {
      // trim leading and trailing '
      String text = ctx.STRING_LITERAL().getText();
      // assert it has leading and trailing '
      assert text.length() >= 2;
      assert text.charAt(0) == '\'';
      assert text.charAt(text.length() - 1) == '\'';
      text = text.substring(1, text.length() - 1);
      return new ConstantExpression(new StringValue(text, text.length()));
    } else if (ctx.K_NULL() != null) {
      return new ConstantExpression(null);
    } else {
      throw new RuntimeException("unknown constant");
    }
  }

  private Expression visitColumnRef(SQLParser.ColumnFullNameContext ctx) {
    if (ctx.tableName() == null) {
      return new ColumnRefExpression(ctx.columnName().getText());
    } else {
      return new ColumnRefExpression(ctx.tableName().getText(), ctx.columnName().getText());
    }
  }

  private Expression visitPrimaryExpr(SQLParser.PrimaryExpressionContext ctx) {
    if (ctx.literalValue() != null) {
      return visitConstants(ctx.literalValue());
    } else if (ctx.columnFullName() != null) {
      return visitColumnRef(ctx.columnFullName());
    } else {
      throw new RuntimeException("unknown primary expression");
    }
  }

  private Expression visitBinaryExpr(SQLParser.ExpressionContext ctx) {
    if (ctx.primaryExpression() != null) {
      return visitPrimaryExpr(ctx.primaryExpression());
    }
    Expression left = visitBinaryExpr(ctx.expression(0));
    Expression right = visitBinaryExpr(ctx.expression(1));
    if (left == null || right == null) {
      throw new RuntimeException("left or right is null");
    }
    // mul & div
    if (ctx.MUL() != null || ctx.DIV() != null) {
      if (ctx.MUL() != null) {
        return new BinaryExpression(left, right, "mul");
      } else {
        return new BinaryExpression(left, right, "div");
      }
    }
    // add & sub
    if (ctx.ADD() != null || ctx.SUB() != null) {
      if (ctx.ADD() != null) {
        return new BinaryExpression(left, right, "add");
      } else {
        return new BinaryExpression(left, right, "sub");
      }
    }
    // comparator
    if (ctx.comparator() != null) {
      String op = ctx.comparator().getText();
      if (op.equals("=")) {
        return new BinaryExpression(left, right, "eq");
      } else if (op.equals(">")) {
        return new BinaryExpression(left, right, "gt");
      } else if (op.equals("<")) {
        return new BinaryExpression(left, right, "lt");
      } else if (op.equals(">=")) {
        return new BinaryExpression(left, right, "ge");
      } else if (op.equals("<=")) {
        return new BinaryExpression(left, right, "le");
      } else if (op.equals("<>")) {
        return new BinaryExpression(left, right, "ne");
      } else {
        throw new RuntimeException("unknown comparator");
      }
    }
    // and & or
    if (ctx.AND() != null || ctx.OR() != null) {
      if (ctx.AND() != null) {
        return new BinaryExpression(left, right, "and");
      } else {
        return new BinaryExpression(left, right, "or");
      }
    }
    throw new RuntimeException("unknown binary expression");
  }

  @Override
  public Statement visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    TableInfo tableInfo = catalog.getTableInfo(ctx.tableName().getText());
    if (tableInfo == null) {
      throw new RuntimeException("table not found");
    }
    Expression where = visitBinaryExpr(ctx.expression());
    return new DeleteStatement(tableInfo, where);
  }

  @Override
  public Statement visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTbStatement(ctx.tableName().getText());
  }

  @Override
  public Statement visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    if (ctx.columnName().size() != ctx.valueEntry().size() && ctx.columnName().size() != 0) {
      throw new RuntimeException("column size not match");
    }
    TableInfo tableInfo = catalog.getTableInfo(ctx.tableName().getText());
    if (tableInfo == null) {
      throw new RuntimeException("table not found");
    }
    List<String> columnNames = new ArrayList<>();
    List<Expression> values = new ArrayList<>();
    Tuple[] tuples;
    int[] uOrderToOrder = new int[tableInfo.getSchema().getColNum()];
    // identical mapping
    for (int i = 0; i < tableInfo.getSchema().getColNum(); i++) {
      uOrderToOrder[i] = i;
    }
    for (int i = 0; i < ctx.columnName().size(); i++) {
      String columnName = ctx.columnName(i).getText();
      int order = tableInfo.getSchema().getColumnOrder(columnName);
      if (order == -1) {
        throw new RuntimeException("column not found");
      }
      uOrderToOrder[order] = i;
    }

    // construct values
    tuples = new Tuple[ctx.valueEntry().size()];
    for (int i = 0; i < ctx.valueEntry().size(); i++) {
      Value<?, ?>[] tupleValues = new Value[tableInfo.getSchema().getColNum()];
      // fill them with null, if column is nullable
      for (int j = 0; j < tableInfo.getSchema().getColNum(); j++) {
        if (tableInfo.getSchema().getColumn(j).Nullable() == 1) {
          tupleValues[j] = tableInfo.getSchema().getColumn(j).getType().getNullValue();
        }
      }
      for (int j = 0; j < ctx.valueEntry(i).literalValue().size(); j++) {
        Expression expression = visitConstants(ctx.valueEntry(i).literalValue(j));
        if (expression instanceof ConstantExpression) {
          tupleValues[uOrderToOrder[j]] =
              tableInfo
                  .getSchema()
                  .getColumn(uOrderToOrder[j])
                  .getType()
                  .castFrom(expression.getValue());
        } else {
          throw new RuntimeException("not constant expression");
        }
      }
      // if we still have null values, that is bad
      for (int j = 0; j < tableInfo.getSchema().getColNum(); j++) {
        if (tupleValues[j] == null) {
          throw new RuntimeException("insert: null value unexpected");
        }
      }
      tuples[i] = new Tuple(tupleValues, tableInfo.getSchema());
    }
    return new InsertStatement(tableInfo, tuples);
  }

  @Override
  public Statement visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    boolean distinct = ctx.K_DISTINCT() != null;
    List<ColumnRefExpression> selectList = new ArrayList<>();
    for (SQLParser.ResultColumnContext rcctx : ctx.resultColumn()) {
      selectList.add(new ColumnRefExpression(rcctx.getText()));
    }
    TableBinder tableBinder = visitTableQLS(ctx.tableQuery());
    Expression where = null;
    if (ctx.expression() != null) {
      where = visitBinaryExpr(ctx.expression());
    }
    return new SelectStatement(
        distinct, selectList.toArray(new ColumnRefExpression[0]), where, tableBinder);
  }

  private TableBinder visitTableQLS(List<SQLParser.TableQueryContext> ctx) {
    if (ctx.size() == 1) {
      return visitTableQ(ctx.get(0));
    }
    // cross product
    TableBinder left = visitTableQ(ctx.get(0));
    TableBinder right = visitTableQLS(ctx.subList(1, ctx.size()));
    Expression on = visitBinaryExpr(ctx.get(0).expression());
    return new JoinTableBinder(left, right, on);
  }

  private TableBinder visitTableQ(SQLParser.TableQueryContext ctx) {
    // no join, regular tablebind
    if (ctx.K_JOIN().size() == 0) {
      TableInfo tableInfo = catalog.getTableInfo(ctx.tableName().get(0).getText());
      if (tableInfo == null) {
        throw new RuntimeException("table not found");
      }
      return new RegularTableBinder(tableInfo);
    }
    // join
    TableBinder left =
        new RegularTableBinder(catalog.getTableInfo(ctx.tableName().get(0).getText()));
    TableBinder right =
        new RegularTableBinder(catalog.getTableInfo(ctx.tableName().get(1).getText()));
    Expression on = visitBinaryExpr(ctx.expression());
    return new JoinTableBinder(left, right, on);
  }

  @Override
  public Statement visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    TableInfo tableInfo = catalog.getTableInfo(ctx.tableName().getText());
    if (tableInfo == null) {
      throw new RuntimeException("table not found");
    }
    // build pair
    String s = ctx.columnName().getText();
    Expression expression = visitBinaryExpr(ctx.expression().get(0));
    Pair<String, Expression> pair = new Pair<>(s, expression);
    // if exists where
    Expression where = null;
    if (ctx.expression().size() == 2) {
      where = visitBinaryExpr(ctx.expression().get(1));
    }
    return new UpdateStatement(tableInfo, pair, where);
  }
}
