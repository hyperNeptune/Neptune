package cn.edu.thssdb.parser.statement;

public enum StatementType {
  INVALID,
  CREATE_DATABASE,
  DROP_DATABASE,
  SHOW_DATABASES,
  USE_DATABASE,
  CREATE_TABLE,
  DROP_TABLE,
  SHOW_TABLES,
  INSERT,
  DELETE,
  UPDATE,
  SELECT,
}
