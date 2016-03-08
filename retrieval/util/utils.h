#ifndef __UTILS_H__
#define __UTILS_H__

#include "def.h"
#include "pool.h"
#include <stdlib.h>
#include <stdio.h>
#include <string>
#define sign_16 ((short)0xC000)
#define exponent_16 ((short)0x3C00)
#define sign_exponent_16 ((short)0x4000)
#define exponent_fill_32 0x38000000
#define mantissa_16 ((short)0x03FF)
#define sign_32 0xC0000000
#define exponent_32 0x07800000
#define mantissa_32 0x007FE000
#define sign_exponent_32 0x40000000
#define loss_32 0x38000000
#define infinite_16 ((short)0x7FFF)
#define infinitesmall_16 (short)0x0000

class Utils {

public:
	
	Utils(void);
	~Utils(void);
	
	static FILE* openinfile(int groupno, int fno, char *path, char *fname);
	static int getNextSize(int targetSize);
	static int rmdirectory(char *path);
	static int copydirectory(char *srcpath, char *despath);
	static int mkdirectory(char *path);
	static int termHash(TermInfo terminfo);
	static int termHash(Term terminfo);
	static int copyFile(char *srcpath, char* descpath);
	static int existFile(char *srcpath);
	static float encode(char *weigth);
	static void decode(float weigth, char* value);
	
};
	
#endif
