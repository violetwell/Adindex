#ifndef UTIL_H
#define UTIL_H
#include <sys/stat.h>
#include <iostream>
using namespace std;
class Util{
public:
  static unsigned long FileSize(const char *FileName);
  static int GetVintSize(unsigned char *Data);
  static int GetVintAndSize(unsigned char *Data, int *size);
  static int GetVint(unsigned char *Data);
  static int GetVlongSize(unsigned char *Data);
  static long GetVlong(unsigned char *Data);
};

#endif
