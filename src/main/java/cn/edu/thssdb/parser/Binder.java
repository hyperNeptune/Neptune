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
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLLexer;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.FloatValue;
import cn.edu.thssdb.type.IntValue;
import cn.edu.thssdb.type.StringValue;
import cn.edu.thssdb.type.Type;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Iterator;

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
      tree = parser1.sqlStmt();
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
      tree = parser2.sqlStmt();
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
    Type type = Type.TYPEDICT.get(typeName);
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
    return new Column(columnName, type, nullable, primary, maxlenth, 0);
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

  private Expression visitConstants(SQLParser.LiteralValueContext ctx) {
    if (ctx.FLOAT_LITERAL() != null) {
      return new ConstantExpression(
          new FloatValue(Float.parseFloat(ctx.FLOAT_LITERAL().getText())));
    } else if (ctx.INTEGER_LITERAL() != null) {
      return new ConstantExpression(
          new IntValue(Integer.parseInt(ctx.INTEGER_LITERAL().getText())));
    } else if (ctx.STRING_LITERAL() != null) {
      return new ConstantExpression(
          new StringValue(ctx.STRING_LITERAL().getText(), ctx.STRING_LITERAL().getText().length()));
    } else if (ctx.K_NULL() != null) {
      return new ConstantExpression(null);
    } else {
      throw new RuntimeException("unknown constant");
    }
  }

  private Expression visitColumnRef(SQLParser.ColumnFullNameContext ctx) {
    String tableName = ctx.tableName().getText();
    String columnName = ctx.columnName().getText();
    if (tableName == null) {
      return new ColumnRefExpression(columnName);
    } else {
      return new ColumnRefExpression(tableName, columnName);
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
    return super.visitDropTableStmt(ctx);
  }

  @Override
  public Statement visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return super.visitShowTableStmt(ctx);
  }

  @Override
  public Statement visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    return super.visitInsertStmt(ctx);
  }

  @Override
  public Statement visitValueEntry(SQLParser.ValueEntryContext ctx) {
    return super.visitValueEntry(ctx);
  }

  @Override
  public Statement visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    return super.visitSelectStmt(ctx);
  }

  @Override
  public Statement visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    return super.visitUpdateStmt(ctx);
  }

  @Override
  public Statement visitResultColumn(SQLParser.ResultColumnContext ctx) {
    return super.visitResultColumn(ctx);
  }

  @Override
  public Statement visitTableQuery(SQLParser.TableQueryContext ctx) {
    return super.visitTableQuery(ctx);
  }

  // TODO: parser to more logical plan
}
