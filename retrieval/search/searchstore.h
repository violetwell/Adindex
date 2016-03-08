#ifndef SEARCH_STORE_H
#define SEARCH_STORE_H
#include "handler.h"
#include "realtime.h"

#define MAX_STORE_LEN 102400
#define STORE_BLOCK_LEN 10000000

class SearchStore {

private:
	unsigned char *StoreLen;
	long StoreLenSize;
	unsigned char *StorePos;
	long StorePosSize;
	unsigned char *StoreDat;
	long StoreDatSize;

	IndexHandler * indexHandle;
	IndexField** indexField;
	int fullTermDocsMaxSize;
	int incTermDocsMaxSize;
	int needSeekFull;
	int needSeekIncr;
	int needSeekRt;
	bool initialized;

private:
	vector<vector<unsigned char> > seekRealtime(Realtime *realTime, vector<unsigned char> Fields, int docId);
	vector<vector<unsigned char> > seekDiskdata(Handler *handle, vector<unsigned char> Fields, int docId);
	vector< vector<unsigned char> > buildResult(unsigned char * databuf, int offset, int len, vector<unsigned char> Fields);

public:
	SearchStore(IndexHandler * handle);
	~SearchStore();
	vector< vector<unsigned char> > seek(int DocID, vector<unsigned char> Fields);
};

#endif
