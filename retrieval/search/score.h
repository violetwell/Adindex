#ifndef  __SCORE_H_
#define  __SCORE_H_

#include "def.h"
#include "handler.h"
#include "term.h"
#include "pool.h"
#include "utils.h"
#include "realtime.h"
#include "indexreader.h"
#include "MinHeap.h"
#include <limits.h>
#include <vector>
#include <stdio.h>

using std::vector;

class Handler;
class IndexHandler;
class Filter;
class DiskConjDocs;
class RealConjDocs;
class DiskTermDocs;
class RealTermDocs;
class MutiTermDocs;
class Combine;
class ConjDocs;
class TermDocs;
class DnfTermDocs;
class ConjDocs;
struct FilterInfo;

typedef struct Pair_Def {
	int type;                              /* full: 0, incr: 1, realtime: 2 */
	int conjId;
} Pair;

typedef struct QueryInfo{
	char * fields;                         /* fieldId of queries */
	char ** contents;                      /* data of query value */
	int * fieldlen;                        /* length of query value */
	int fieldNum;                          /* number of queries */
} QueryInfo;

/* combine post list of different assignments of a dnf-field */
class InnerCombine {
	
public:
	InnerCombine(DnfTermDocs **_streams, int _size);
	~InnerCombine();
	bool next();
	int doc();
	bool belong();
	bool skipTo(int target);
	
private:
	DnfTermDocs **streams;
	int endnum;
	int size;
	char *mark;
	char *belongmark;
	int begin;
	bool fillflag;
	int markflag;
	void fill();
	void init();
public:
	int curdoc;
	bool curbelong;
	
};

class DocIterator {
	
public:
	DocIterator(IndexHandler *_handlers);
	virtual ~DocIterator();
	virtual int docId() = 0;
	virtual bool next() = 0;
	virtual bool skipTo(int target) = 0;
	virtual float score() = 0;
	
public:
	int docid;                                              /* current docId */
	float cscore;                                           /* WAND score of docId */
	IndexHandler *handlers;
	
};

/* docId list of which dnf field is empty */
class NoDnfDocIterator : public DocIterator {
	
public:
	NoDnfDocIterator(IndexHandler *_handlers,  FilterInfo * _filterInfo);
	~NoDnfDocIterator();
	int docId();
	bool next();
	bool skipTo(int target);
	float score();
	
private:
	void init();
	int state;
	ConjDocs **docs;
	Combine *combine;
	Filter **filters;
	FilterInfo * filterInfo;
};

/* docId post list of a term, post list includes full, incr, realtime */
class TermDocIterator : public DocIterator {
	
public:
	TermDocIterator(IndexHandler *_handlers, char _field, char *_data, int _dataLen, FilterInfo * _filterInfo);
	~TermDocIterator();
	int docId();
	bool next();
	bool skipTo(int target);
	float score();
	
private:
	void init();
	int state;
	MutiTermDocs *muti;
	TermDocs **docs;
	Term term;
	FilterInfo * filterInfo;
	Filter ** filters;

};

/* docId list for result of dnf query. First search conjIds which fits dnf-query, then search docIds which fits filter and not deleted */
class DnfDocIterator : public DocIterator {
	
public:
	DnfDocIterator(IndexHandler *_handlers, char *_data, int _dataLen, FilterInfo * _filterInfo);
	~DnfDocIterator();
	int docId();
	bool next();
	bool skipTo(int target);
	float score();
	
private:
	char *data;                                            /* dnf query */
	int dataLen;
	int conjsize;                                          /* size of query */
	int state;
	char *zeroterm;
	Term zero;
	Term *terms;                                           /* dnf terms */
	int *subnums;                                          /* subnums is the count of assignments for different dnf-field */
	vector<Pair> conjs;                                    /* conjIds which fits dnf query */
	ConjDocs **conjdocs;                                   /* list of docIds which fits dnf, filter query and not deleted, in full, incr and realtime */
	Combine *combine;                                      /* a combine of doc list in conjdocs */
	int conjdocnum;
	FilterInfo * filterInfo; 
	Filter ** filters; 

	void init();
	void queryconj(DnfTermDocs **docs, int *subNums, int subNumSize, int level, int type);
	int compareTermDocs(InnerCombine *p1, InnerCombine *p2);
	void quickSort(InnerCombine **termDocs, int lo, int hi);
};

/* docId post list for result of tag query. First get doc lists of all tag-terms, then use WAND to build the doc list */
class TagDocIterator : public DocIterator {
	
public:
	TagDocIterator(IndexHandler *_handlers, DocIterator *_it, char *_data, int _dataLen, MinHeap * _minHeap, FilterInfo * _filterInfo);
	~TagDocIterator();
	int docId();
	bool next();
	bool skipTo(int target);
	float score();
	
private:
	DocIterator *it;
	char *data;
	int dataLen;
	MutiTermDocs **docs;
	int size;                                                  /* size of docs */
	int state;
	float threod;                                              /* threshold of WAND */
	bool firstTime;
	float *queryweights;
	FilterInfo * filterInfo;
	Filter ** filters;
	MinHeap * minHeap;
	int ** hashTable;

	void init();
	void iload();
	bool donext();
	int compareTermDocs(MutiTermDocs *p1, MutiTermDocs *p2);
	void quickSort(MutiTermDocs **termDocs, int lo, int hi);
};

#endif
