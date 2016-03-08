#ifndef  __DOCPARSER_H_
#define  __DOCPARSER_H_

#include "def.h"
#include "utils.h"
#include "fields.h"

class BufferedReader {
	
public:
	BufferedReader(FILE *_file, int _size);
	~BufferedReader();
	int read();
	int read(char *b, int off, int len);
	void close();
	float readUb();
	long readVLong();
	int readVInt();
	long readLong();
	int readInt();
	
private:
	int size;
	char* buf;
	int count;
	int pos;
	FILE *file;
	void fill();
	int read1(char *b, int off, int len);
	
};

class DocParser {
	
public:
	DocParser(IndexSchema *_schema);
	DocParser(int _groupno, char *_path, IndexSchema *_schema);
	~DocParser();
	bool hasNext();                                                              /* load next doc */
	char* data(int index);
	int length(int index);
	bool initData(char* bindata, int datalen);
	
private:
	int groupno;
	char *path;
	IndexSchema *schema;
	
	IndexField **fieldArray;
	FILE *rawfp;
	int fileno;
	int number;

	BufferedReader *br;
	int cursor;
	char* dataBuffer;                                                            /* store data of fields */
	char** dataArray;                                                            /* dataArray[i] points to the data of ith field */
	int* lenArray;                                                               /* lenArray[i] is the length of ith field */
	
	int readFix(int index, int length);
	int readBinary(int index);
	int readFix(char *bindata, int begin, int datalen, int index, int length);
	int readBinary(char *bindata, int begin, int datalen, int index);
	
};

#endif


