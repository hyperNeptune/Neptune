package cn.edu.thssdb.type;

public enum BuiltinTypeCode {
  INT((byte) 0),
  LONG((byte) 1),
  FLOAT((byte) 2),
  DOUBLE((byte) 3),
  STRING((byte) 4);

  public byte value;

  BuiltinTypeCode(byte value) {
    this.value = value;
  }
}
