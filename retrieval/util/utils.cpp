#include "utils.h"

Utils::Utils() {
}

Utils::~Utils() {
}

FILE* Utils::openinfile(int groupno, int fno, char *path, char *fname) {
	char tpfile[MAX_DIR_LEN];
	FILE *fp;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d.%d", path, fname, groupno, fno);
	fp = fopen(tpfile, "r");
	return fp;
}

int Utils::getNextSize(int targetSize) {
	return (targetSize >> 3) + (targetSize < 9 ? 3 : 6) + targetSize;
}

int Utils::rmdirectory(char *path) {
	if (path == NULL || strcmp(path, "/") == 0) {
		return 0;
	}
	char *temp = (char*)malloc(sizeof(char) * 1024);
	char *cmd = "rm -rf ";
	strcpy(temp, cmd);
	strcpy(temp + strlen(cmd), path);
	system(temp);
	free(temp);
	return 1;
}

int Utils::mkdirectory(char *path) {
	if (path == NULL || strcmp(path, "/") == 0) {
		return 0;
	}
	char *temp = (char*)malloc(sizeof(char) * 1024);
	char *cmd = "mkdir -p ";
	strcpy(temp, cmd);
	strcpy(temp + strlen(cmd), path);
	system(temp);
	free(temp);
	return 1;
}

int Utils::copydirectory(char *srcpath, char *despath) {
	if (srcpath == NULL || despath == NULL 
	|| strcmp(srcpath, "/") == 0 || strcmp(despath, "/") == 0) {
		return 0;
	}
	char *temp = (char*)malloc(sizeof(char) * 2048);
	char *cmd = "cp -r ";
	strcpy(temp, cmd);
	int len = strlen(cmd);
	strcpy(temp + len, srcpath);
	len += strlen(srcpath);
	temp[len++] = '*';
	temp[len++] = ' ';
	strcpy(temp + len, despath);
	system(temp);
	free(temp);
	return 1;
}

int Utils::termHash(TermInfo terminfo) {
	int i = 0;
	int hash = 0;
	for (i = terminfo.dataLen; i > 0; i--) {
		hash = 31 * hash + terminfo.data[i - 1];
	}
	hash = terminfo.field + 31 * hash;
	return hash;
}

int Utils::termHash(Term terminfo) {
	int i = 0;
	int hash = 0;
	char *data = (char*)terminfo.data;
	for (i = terminfo.dataLen; i > 0; i--) {
		hash = 31 * hash + data[i - 1];
	}
	hash = (char)terminfo.field + 31 * hash;
	return hash;
}

int Utils::copyFile(char *srcpath, char* descpath) {
	if (srcpath == NULL || descpath == NULL 
	|| strcmp(srcpath, "/") == 0 || strcmp(descpath, "/") == 0) {
		return 0;
	}
	FILE *tp = fopen(srcpath, "r");
	if (tp == NULL) return 0;
	char *temp = (char*)malloc(sizeof(char) * 2048);
	char *cmd = "cp -f ";
	int length = 0;
	strcpy(temp, cmd);
	length += strlen(cmd);
	strcpy(temp + length, srcpath);
	length += strlen(srcpath);
	temp[length++] = ' ';
	strcpy(temp + length, descpath);
	system(temp);
	free(temp);
	return 1;
}

int Utils::existFile(char *srcpath) {
	FILE *tp = fopen(srcpath, "r");
	if (tp == NULL) return 0;
	else return 1;
}

float Utils::encode(char *weigth) {
	if (weigth == NULL) return 0;
	short s = 0;
	char *srtPtr = (char*)&s;
	srtPtr[0] = weigth[0];
	srtPtr[1] = weigth[1];
	int sign = ((int)(s & sign_16)) << 16;
	int exponent = ((int)(s & exponent_16)) << 13;
	if((s & sign_exponent_16) == 0 && s != 0) {
		exponent |= exponent_fill_32;
	}
	int mantissa = ((int)(s & mantissa_16)) << 13;
	int code = sign | exponent | mantissa;
	float *fat = (float*)&code;
	return *fat;
}

void Utils::decode(float weigth, char* value) {
	int *fi = (int*)&weigth;
	short sign = (short)((*fi & sign_32) >> 16);
	short exponent = (short)((*fi & exponent_32) >> 13);
	short mantissa = (short)((*fi & mantissa_32) >> 13);
	short code = (short)(sign | exponent | mantissa);
	short result = 0;
	if((*fi & loss_32) > 0 && (*fi & sign_exponent_32) > 0) {
		result = (short)(code | infinite_16);
	}
	if((*fi & loss_32 ^ loss_32) > 0 && (*fi & sign_exponent_32) == 0) {
		result = infinitesmall_16;
	}
	result = code;
	value[0] = ((char*)&result)[0];
	value[1] = ((char*)&result)[1];
}
