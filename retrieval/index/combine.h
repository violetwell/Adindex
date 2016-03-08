#ifndef  __INDEXREADER_H_
#define  __INDEXREADER_H_

#include "utils.h"
#include "pool.h"

#define DOC_ID_BYTE_NUM 4
#define DOC_WEIGHT_BYTE_NUM 2
#define CONJ_ID_BYTE_NUM 4
#define CONJ_BELONG_BYTE_NUM 1
#define BITMAP_BLOCK_SIZE 10240
//#define MAX_CONJ_SIZE 1024
#define JUMP_STEP(l)  ((l/40 < 1) ? 10 : l/40)


class Combine {
	
public:
	Combine(ConjDocs **_docs, int _size);
	~Combine();
	int docId();
	bool next();
	int skipTo(int target);
	
private:
	ConjDocs **docs;
	int size;
	int docid;
	int begin;                                           
	int end;     
	bool fillFlag;                                       
	int markFlag;
	bool isInit;
	int conjListEndNum; 
	//int conjListEnd[MAX_CONJ_SIZE];
	int * conjListEnd;
	char mark[BITMAP_BLOCK_SIZE];
	bool fill();
	bool init();
	
};

