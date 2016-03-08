#ifndef SEARCH_FILTER_H
#define SEARCH_FILTER_H
#include "handler.h"
#include "realtime.h"

#define MAX_STORE_LEN 102400
#define MAX_CONTENT_SIZE 1
#define STORE_BLOCK_LEN 10000000
#define PERIODS_FIELD 0X00
#define PERIODS_BYTE_NUM 16
#define CONT_TIME_FIELD 0X05


class RAMInputStream;
class Handler;
class Realtime;
class FilterStrategy;

typedef struct FilterInfo {
	char *filedIds;                            /* field id of filter query */
	char ** contents;                          /* data of filter query value, binary mode */
	int * contLen;
	int filedNum;                              /* count of not-null filter queries */
} FilterInfo;

class Filter {

public:
	Filter();
	virtual ~Filter();
	virtual bool filterData(int id) = 0; 
};

class DiskBrifeFilter : public Filter {

private:
	unsigned char * brifeData;
	char * content;
	int len;
	int fid;
	int isInit;

public:
	DiskBrifeFilter(unsigned char * _brife, int _len, int _fid, char * _content);
	~DiskBrifeFilter();
	bool filterData(int id);
};

class DiskStoreFilter : public Filter {

private:
	Handler *handle;
	unsigned char * storePos;
	unsigned char * storeLen;
	unsigned char * storeDat;
	long storeLenSize;
	long storePosSize;
	long storeDatSize;
	FilterInfo * filterInfo;
	FilterStrategy * filterStrategy;
	int * fieldLen;
	int size;
	bool isInit;

public:
	DiskStoreFilter(Handler * _handle, FilterInfo * _filterInfo, int * _fieldlen, int _size);
	~DiskStoreFilter();
	bool filterData(int id);
};

class DiskFilter : public Filter {
	
private:

	Handler *handle;
	unsigned char * delFile;
	long delSize;
	vector<DiskBrifeFilter *> brifes;
	DiskStoreFilter * store;
	FilterInfo * filterInfo;
	FilterInfo * storeFilterInfo;
	IndexField** indexField;
	int storeSize;
	int * fieldLen; 
	int docnum; 
	bool isInit;

	bool isDelete(int id);

public:
	DiskFilter(Handler *_handle, FilterInfo * _filterInfo);
	~DiskFilter();
	bool filterData(int id);
};


class RealBrifeFilter : public Filter {

private:
	RAMInputStream * brifeDataStream;
	char * content;
	char * brifeData;
	int len;
	int fid;
	int isInit;

public:
	RealBrifeFilter(RAMInputStream * _brife, int _len, int _fid, char * _content);
	~RealBrifeFilter();
	bool filterData(int id);
};

class RealStoreFilter : public Filter {

private:
	Realtime * realtime;
	RAMInputStream * storeLenStream;
	RAMInputStream * storePosStream;
	RAMInputStream * storeDatStream;
	FilterInfo * filterInfo;
	FilterStrategy * filterStrategy;	
	unsigned char datBytes[MAX_STORE_LEN];
	int size;
	int * fieldLen;
	bool isInit;

	int time1;
	int time2;

public:
	RealStoreFilter(Realtime * _realtime, FilterInfo * _filterInfo, int * _fieldlen, int _size);
	~RealStoreFilter();
	bool filterData(int id);
};

class RealFilter : public Filter {

private:
	Realtime * realtime;
	vector<RealBrifeFilter *> brifes;
	RealStoreFilter * store;
	FilterInfo * filterInfo;
	FilterInfo * storeFilterInfo;
	IndexField** indexField;
	int storeSize;
	int * fieldLen;
	int docnum;
	bool isInit;

public:
	RealFilter(Realtime * realtime, FilterInfo * filterInfo);
	~RealFilter();
	bool filterData(int id);
};

class FilterStrategy {

private:
	FilterInfo * filterInfo;	
public:
	FilterStrategy(FilterInfo * _filterInfo);
	~FilterStrategy();
	bool returnStrategy(int fid);
	bool filterStoreMethod(char * storeData, int storeLen, int idx);
	bool filterBrifeMethod(char * brifeData, int len, char *content, int fid);
};

#endif
