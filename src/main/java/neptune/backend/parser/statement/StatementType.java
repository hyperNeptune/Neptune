package neptune.backend.parser.statement;

public enum StatementType {
  INVALID,
  CREATE_TABLE,
  DROP_TABLE,
  SHOW_TABLE,
  INSERT,
  DELETE,
  UPDATE,
  SELECT,
}
