// This piece of code is (maybe not even) only guaranteed to work on Linux, and is not portable at all.
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

void check(int retValue) {
  if (retValue) {
    printf("test failed\n");
    exit(-1);
  }
}

void notNULL(void *ptr) {
  if (ptr == NULL) {
    printf("test failed\n");
    exit(-1);
  }
}

// ensure target exists
int target_notExists() {
  if (access("../target/client-jar-with-dependencies.jar", F_OK) != 0) {
    printf("target client not found\n");
    return -1;
  }
  if (access("../target/server-jar-with-dependencies.jar", F_OK) != 0) {
    printf("target server not found\n");
    return -1;
  }
  printf("HyperNeptune target found\n");
  return 0;
}

// ensure java exists, and is version 8
int java_exists() {
  FILE *fp;
  char path[1035];
  int found = 0;

  /* Open the command for reading. */
  fp = popen("java -version 2>&1 | grep \"version \\\"1.8\"", "r");
  if (fp == NULL) {
    printf("Failed to run command\n");
    return -1;
  }

  /* Read the output a line at a time - output it. */
  while (fgets(path, sizeof(path) - 1, fp) != NULL) {
    printf("Java detected: %s", path);
    found++;
  }

  if (!found) {
    printf("java version 1.8 not found\n");
    return -1;
  }

  /* close */
  pclose(fp);
  return 0;
}

// open directory test_cases, list all files
// get *.case, and *.expect (check all .case should have a .expect)
// store their name in a char* array
int load_tests(char ***test_cases) {
  // open directory
  DIR *dir;
  struct dirent *ent;
  char *tests[8192];
  notNULL(dir = opendir("./test_cases"));

  // print all the files and directories within directory
  int count = 0;

  while ((ent = readdir(dir)) != NULL) {
    char *dot = strrchr(ent->d_name, '.');
    if (dot != NULL && strcmp(dot, ".case") == 0) {
      int fpresz = strlen(ent->d_name) - 5;
      tests[count] = calloc(fpresz + 1, 1);
      memcpy(tests[count], ent->d_name, fpresz);
      tests[count][fpresz] = '\0';
      printf("case: %s\n", tests[count++]);
    } else {
      // printf("not a test case: %s\n", ent->d_name);
      continue;
    }
  }
  closedir(dir);
  printf("total %d cases\n\n", count);
  *test_cases = tests;
  return count;
}

/*
 readline using fd
*/
int readline(int fd, char *buf, int size) {
  int i = 0;
  char c;
  while (i < size - 1) {
    int ret = read(fd, &c, 1);
    if (ret <= 0) {
      return 0;
    }
    if (c == '\n') {
      buf[i] = '\n';
      buf[++i] = '\0';
      return i;
    }
    buf[i++] = c;
  }
  return i;
}

int run_test(char *test_case) {
  // new 2 processes
  // 1. run server
  // 2. run client
  // wait for both to finish
  // check if client output matches expected output
  // each row in .case is a command, run it in client
  // each row in .expect is a line, check if client output matches
  // if not, print error message
  // return 1 if test fails, 0 if test passes

  // new server process
  pid_t serverpid = 0;
  switch (serverpid = fork()) {
  case -1:
    printf("fork failed\n");
    return -1;
  case 0:
    // child process
    printf("running server\n");
    execlp("java", "java", "-jar", "../target/server-jar-with-dependencies.jar",
           NULL);
    printf("server failed to run\n");
    return -1;
  default:
    break;
  }

  // wait server launch
  sleep(1);

  // prepare two files for input & output
  test_case = realloc(test_case, strlen(test_case) + 10);
  strcat(test_case, ".case");
  char fbuf[8192] = "test_cases/";
  strcat(fbuf, test_case);
  int inputfile = open(fbuf, O_RDONLY, 0666);
  int outputfile =
      open("test_cases/test.out", O_RDWR | O_CREAT | O_TRUNC, 0666);
  int pipefd[2];

  if (pipe(pipefd) == -1) {
    printf("pipe failed\n");
    goto bad;
  }

  // client
  pid_t clientpid = 0;
  switch (clientpid = fork()) {
  case -1:
    printf("fork failed\n");
    goto bad;
  case 0:
    // child process
    close(pipefd[1]);
    printf("running client %s\n", test_case);
    // redirect stdout to file test.out
    // redirect stdin to file test.in
    dup2(pipefd[0], STDIN_FILENO);
    dup2(outputfile, STDOUT_FILENO);
    freopen("/dev/null", "w", stderr);
    execlp("java", "java", "-jar", "../target/client-jar-with-dependencies.jar",
           test_case, NULL);
    printf("client failed to run\n");
    return -1;
  default:
    break;
  }

  close(pipefd[0]);

  // read from input file, write to pipe
  char buf[8192];
  int l;
  while ((l = readline(inputfile, buf, 8192))) {
    sleep(1);
    printf("send command: %s", buf);
    // wait client launch
    write(pipefd[1], buf, l);
    sleep(1);
  }

  // recycle
  kill(serverpid, SIGTERM);
  kill(clientpid, SIGTERM);
  waitpid(serverpid, NULL, 0);
  waitpid(clientpid, NULL, 0);

  // close pipe
  close(pipefd[1]);
  // close files
  close(inputfile);
  close(outputfile);

  // compare output file with expect file
  // if not match, print error message
  // ignore the line starts with "It costs"
  char *expectfile = calloc(8192, 1);
  strcat(expectfile, "test_cases/");
  strncat(expectfile, test_case, strlen(test_case) - 5);
  strcat(expectfile, ".expect");
  int expectfd = open(expectfile, O_RDONLY, 0666);
  int outputfd = open("test_cases/test.out", O_RDONLY, 0666);
  char expectbuf[8192];
  char outputbuf[8192];
  int expectl, outputl;
  int line = 0;
  while ((expectl = readline(expectfd, expectbuf, 8192)) &&
         (outputl = readline(outputfd, outputbuf, 8192))) {
    line++;
    if (strncmp(expectbuf, "It costs", 8) == 0) {
      continue;
    }
    if (strcmp(expectbuf, outputbuf) != 0) {
      printf("test case %s failed at line %d\n", test_case, line);
      printf("expected: %s", expectbuf);
      printf("actual: %s", outputbuf);
      free(expectfile);
      goto bad;
    }
  }

  close(expectfd);
  close(outputfd);
  // remove test.out
  remove("test_cases/test.out");
  return 0;
bad:
  remove("test_cases/test.out");
  return -1;
}

// run all test cases
int run_all_tests(int *all_tests) {
  char **test_cases;
  int test_passes = 0;
  *all_tests = load_tests(&test_cases);
  // run all test cases
  for (int i = 0; i < *all_tests; i++) {
    printf("running test case %s\n", test_cases[i]);
    test_passes += !run_test(test_cases[i]);
  }
  return test_passes;
}

int main() {
  check(target_notExists());
  check(java_exists());
  // load all test cases
  printf("=====================\n"
         "Running test cases...\n"
         "=====================\n");
  int all_tests;
  int test_passes = run_all_tests(&all_tests);
  printf("=====================\n"
         "%d / %d test cases passed\n"
         "=====================\n",
         test_passes, all_tests);
}