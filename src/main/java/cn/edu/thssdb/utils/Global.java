package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;
  public static String DEFAULT_USER_NAME = "root";
  public static String DEFAULT_PASSWORD = "root";

  public static String CLI_PREFIX = "ThssDB2023>";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect;";
  public static final String QUIT = "quit;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  // Type Package Constants
  public static final int INT_SIZE = 4;
  public static final int LONG_SIZE = 8;
  public static final int FLOAT_SIZE = 4;
  public static final int DOUBLE_SIZE = 8;
  public static final int BOOL_SIZE = 1;

  // MAX & MIN value for types
  public static final int INT_MAX = 2147483647;
  public static final int INT_MIN = -2147483648;
  public static final long LONG_MAX = 9223372036854775807L;
  public static final long LONG_MIN = -9223372036854775808L;
  public static final float FLOAT_MAX = 3.4028235E38f;
  public static final float FLOAT_MIN = -3.4028235E38f;
  public static final double DOUBLE_MAX = 1.7976931348623157E308;
  public static final double DOUBLE_MIN = -1.7976931348623157E308;

  // Disk Manager Constants
  public static final int PAGE_SIZE = 4096;
  public static final int PAGE_ID_INVALID = -1;

  // buffer pool manager Constants
  public static final int DEFAULT_BUFFER_SIZE = 4096;
}
