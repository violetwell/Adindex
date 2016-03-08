#ifndef  __INDEXREADER_H_
#define  __INDEXREADER_H_

#include "utils.h"
#include "pool.h"
#include "filter.h"
#include <limits.h>

#define DOC_ID_BYTE_NUM 4
#define DOC_WEIGHT_BYTE_NUM 2
#define CONJ_ID_BYTE_NUM 4
#define CONJ_BELONG_BYTE_NUM 1
#define BITMAP_BLOCK_SIZE 10240
#define MAX_CONJ_SIZE 1024
#define JUMP_STEP(l)  ((l/400 < 1) ? 10 : l/400)
#define ISDELETE(id, deldata) ((((*(char *)(deldata + id/8)) & (1<<(7-id%8))) == (1<<(7-id%8))) ? 1 : 0)

class Filter;
class RtByteSliceReader;

class ConjDocs {
	
public:
	ConjDocs(int _base);
	virtual ~ConjDocs();
	virtual int docId() = 0;
	virtual bool next() = 0;
	
public:
	int docid;
	int base;
	
};

/* read conj.dat */
class DiskConjDocs : public ConjDocs {
	
public:
	DiskConjDocs(int _base, char* _data, char* _deldata, int _docnum, Filter * _filter);
	~DiskConjDocs();
	int docId();
	bool next();
	
private:
	char *data;
	char *deldata;
	int docnum;
	bool flag;
	Filter * filter;	
	
//	bool isDelete(int id);
};

class RealConjDocs : public ConjDocs{
	
public:
	RealConjDocs(int _base, int _maxdoc, RtByteSliceReader *_reader, Realtime *_rtime, Filter * _filter);
	~RealConjDocs();
	int docId();
	bool next();
	
private:
	int maxdoc;
	RtByteSliceReader *reader;
	Realtime *rtime;
	bool flag;
	Filter * filter;
};

/* combine ConjDocs */
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
	int begin;                                           
	int end;     
	bool fillFlag;                                       
	int markFlag;
	bool isInit;
	int conjListEndNum; 
	int * conjListEnd;
	char mark[BITMAP_BLOCK_SIZE];
	bool fill();
	bool init();

public:
	int docid;
	
};

class DnfTermDocs {
	
public:
	DnfTermDocs();
	virtual ~DnfTermDocs();
	virtual int conjId() = 0;
	virtual bool belong() = 0;
	virtual bool next() = 0;
	virtual int skipTo(int target) = 0;
	
public:
	int conjid;
	bool isbelong;
	
};

class DiskDnfTermDocs : public DnfTermDocs {
	
public:
	DiskDnfTermDocs(char *_data, int _conjnum);
	~DiskDnfTermDocs();
	int conjId();
	bool belong();
	bool next();
	int skipTo(int target);
	
private:
	char *data;
	int conjnum;
	bool flag;
	
};

class RealDnfTermDocs : public DnfTermDocs {
	
public:
	RealDnfTermDocs(int _maxconj, RtByteSliceReader *_reader);
	~RealDnfTermDocs();
	int conjId();
	bool belong();
	bool next();
	int skipTo(int target);
	
private:
	int maxconj;
	RtByteSliceReader *reader;
	bool flag;
	
};

class TermDocs {
	
public:
	TermDocs(float _ub, int _base);
	TermDocs();
	virtual ~TermDocs();
	virtual int docId() = 0;
	virtual float weight() = 0;
	virtual bool next() = 0;
	virtual int skipTo(int target) = 0;
	float getub();
	int getbase();
	
public:
	int docid;                                         /* current docID */
	char wei[3];                                       /* weight */
	float ub;
	int base;
	
};

/* combination of TermDocs of full, incr, realtime */
class MutiTermDocs : public TermDocs {
	
public:
	MutiTermDocs(TermDocs **_termdocs, int _size);
	~MutiTermDocs();
	int docId();
	float weight();
	bool next();
	int skipTo(int target);

	float queryweight;                                    /* weight of term in query */
	
private:
	TermDocs **termdocs;
	int size;
	int curoff;                                           /* index of current termdoc in termdocs */
	bool flag;
	TermDocs *curtermdoc;                                 /* current termdoc considering */
	
};

/* post list of term in disk */
class DiskTermDocs : public TermDocs {
	
public:
	DiskTermDocs(float _ub, int _base, char *_data, char* _deldata, int _docnum, Filter *_filter);
	~DiskTermDocs();
	int docId();
	float weight();
	bool next();
	int skipTo(int target);
	
private:
	char *data;
	char *deldata;
	int docnum;	
    bool flag;
	Filter * filter;
	bool jumpTable(int target);
	int getJumpPos(int beg, int end, int to);
	int compareTo(int beg,int mid, int to);
//	bool isDelete(int id);
};

/* post list of term in realtime */
class RealTermDocs : public TermDocs {
	
public:
	RealTermDocs(float _ub, int _base, int _maxdoc, RtByteSliceReader *_reader, Realtime *_rtime, Filter *_filter);
	~RealTermDocs();
	int docId();
	float weight();
	bool next();
	int skipTo(int target);
	
private:
	int maxdoc;
	RtByteSliceReader *reader;
	Realtime * rtime;
	bool flag;
	Filter * filter;
};

int readVint(RtByteSliceReader *reader);
float readWeight(RtByteSliceReader *reader);

#endif
