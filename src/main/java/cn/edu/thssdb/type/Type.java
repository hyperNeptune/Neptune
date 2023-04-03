package cn.edu.thssdb.type;

// THE CANANICAL NAME OF THESE TYPES ARE:
// INTEGER, BIGINT, DECIMAL, VARCHAR
public enum Type {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING;
  // toString
  @Override
  public String toString() {
    switch (this) {
      case INT:
        return "INT";
      case LONG:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case STRING:
        return "STRING";
      default:
        return "UNKNOWN";
    }
  }
};
