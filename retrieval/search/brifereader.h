#ifndef SEARCH_BRIFE_H
#define SEARCH_BRIFE_H
#include "handler.h"
#include "realtime.h"


class RAMInputStream;
class Handler;
class Realtime;

class BrifeReader {

public:
	BrifeReader();
	virtual ~BrifeReader();
//	virtual bool getRealBrife(int docid, int fid, char* retbuf, int len) = 0; 
//	virtual char* getDiskBrife(int docid, int fid, int len) = 0;  
};

class DiskBrifeReader : public BrifeReader {

private:
	vector<unsigned char*> brifes;                            /* brifes[i] store the brife data of ith field in shema.xml */
	vector<int> fieldlen;                                     /* fieldlen[i] store the length of brife data */
	IndexField** indexField;
	int size;                                                 /* schema size */
	Handler *handle;
	int docnum;
	int base;                                                 /* docId base */
	bool isInit;

public:
	DiskBrifeReader(Handler *handle, vector<int> fields, int base);
	~DiskBrifeReader();
	char* getDiskBrife(int docid, int fid, int len);
};

class RealBrifeReader : public BrifeReader {

private:
	vector<int> fields;
    vector<RAMInputStream *> brifes;
	vector<int> fieldlen;
	Realtime * realtime;
	IndexField** indexField; 
	int size;
	int docnum;
	int base;
	bool isInit;

public:
	RealBrifeReader(Realtime * realtime, vector<int> fields, int base);
	~RealBrifeReader();
	bool getRealBrife(int docid, int fid, char* retbuf, int len);
};

#endif
