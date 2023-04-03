package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.Type;

public class Column implements Comparable<Column> {
  private String name_;
  private Type type_;
  private Boolean primary_;
  private Boolean nullable_;
  private int maxLength_;
  // offset in a tuple
  protected int offset_;

  public Column(String name, Type type, Boolean primary, boolean nullable, int maxLength) {
    this.name_ = name;
    this.type_ = type;
    this.primary_ = primary;
    this.nullable_ = nullable;
    this.maxLength_ = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name_.compareTo(e.name_);
  }

  public String toString() {
    return name_ + ',' + type_ + ',' + primary_ + ',' + nullable_ + ',' + maxLength_ + ',' + offset_;
  }

  // getters
  public String getName() {
    return name_;
  }

  public Type getType() {
    return type_;
  }

  public Boolean isPrimary() {
    return primary_;
  }

  public Boolean Nullable() {
    return nullable_;
  }

  public int getMaxLength() {
    return maxLength_;
  }

  public int getOffset() {
    return offset_;
  }

}
