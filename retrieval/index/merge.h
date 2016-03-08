#ifndef  __MERGE_H_
#define  __MERGE_H_

#include <assert.h>
#include "def.h"
#include "adindex.h"
#include "docparser.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"

class Merger {
	
public:
	Merger(void *_conf, char *_outputPath, bool _termDict, int _groupNum, int _outGroup);
	virtual ~Merger();
	int merge();
	int dnfInverted();
	int inverted();
	TermDat* getTerm(int group, int termno);
	int termCmp(TermDat *a, TermDat *b);
	int mergeTermIndex();
	TermDat* getDnfTerm(int group, int termno);
	int mergeDnfTermIndex();
	
protected:
	virtual int init()=0;
	virtual int over()=0;
	virtual int brife()=0;
	virtual int store()=0;
	IndexSchema *schema;
	char *outputPath;
	bool termDict;
	int indexFieldNumber;                                    /* count of fields in indexField */
	IndexField **indexField;                                 /* store index field in schema.xml */
	int brifeFieldNumber;
	IndexField **brifeField;
	int storeFieldNumber;
	IndexField **storeField;
	int groupNum;
	int outGroup;
	
	int *docNums;                                            /* docNums[i] is the doc count of group i, including deleted docs */
	int samenum;                                             /* the count of same term while merging term, 1 means no same term */
	int *termNums;                                           /* termNums[i] is the term count of group i */
	MergeInfo *mergeinfo;
	TermDat **loadTerms;                                     /* loadTerms[i] buffers the term of group i */
	int *readedTermNum;                                      /* readedTermNum[i] is the buffered term count of group i */
	BufferedReader **termReaders;                            /* termReaders[i] reads the term.dat of group i */
	BufferedReader **indexReaders;                           /* indexReaders[i] reads the index.dat of group i */
	char **termBuffer;                                       /* termBuffer[i] buffers the term name of group i */
	FILE *termFp;                                            /* term.dat in fullindex */
	FILE *indexFp;                                           /* index.dat in fullindex */
	FILE *termSkipFp;                                        /* term.idx in fullindex */
	long termWrite;                                          /* bytes written in term.dat, used to write term.idx */
	long indexWrite;                                         /* bytes written in index.dat */
	int lastWrite;                                           /* length of last term written, used to write term.idx */
	int mergedNum;                                           /* term written, used to write term.idx */
	int termWriteLen;
	char *termWriteBuffer;
	char *writeBuffer;
	
	virtual int writeIndex(int objno, int group, int num)=0;
	virtual int writeDnfIndex(int group, int num)=0;
	virtual void inpath(char *tp, char* name, int group)=0;
	
};

class FullMerger : public Merger {
	
public:
	FullMerger(void *_conf, char *_outputPath, bool _termDict, 
	char *_conjPath, char *_inputPath, int *_groups, int _groupNum, int _outGroup);
	~FullMerger();
	
private:
	char *conjPath;
	int *groups;
	char *inputPath;
	
protected:
	virtual int init();
	virtual int over();
	virtual int brife();
	virtual int store();
	virtual void inpath(char *tp, char* name, int group);
	virtual int writeIndex(int objno, int group, int num);
	virtual int writeDnfIndex(int group, int num);
	
};

class IncrMerger : public Merger {
	
public:
	IncrMerger(void *_conf, char *_outputPath, bool _termDict, 
	char *_incrPath, char *_realtimePath, char *_incrLevel, char *_realtimeLevel);
	~IncrMerger();
	
private:
	char *incrPath;
	char *realtimePath;
	char *incrLevel;
	char *realtimeLevel;
	leveldb::Slice *sizeKey;
	char **inputPaths;
	int **newDocIds;                                   /* newDocIds[i] is array of newly arranged docIds, -1 for deleted docs. i=0:incr, i=1:realtime */
	int *newConjIds;                                   /* for conj in realtime, newConjIds[conjId] = 0-conIdIncr if found in incr, newConjIds[conjId] is newly assigned conjId increasing afer conjIds in incr if not found in incr */
	int *newDocNums;                                   /* newDocNums[i] is count of docId in newDocIds[i] */
	int incrConj;                                      /* count of conj in incr */
	int realtimeConj;                                  /* count of conj in realtime */
	MergeConjInfo **conjInfo;                          /* conjInfo[i] store conj.idx of group i */
	
	Store_Pos **posarr;                                /* posarr[i] store store.pos of group i */
	Store_Length **lengtharr;                          /* lengtharr[i] store store.len of group i */
	char *srcbuf;                                      /* buffer of store.dat while merging */
	int writebrife(FILE *brifeInFp, FILE *brifeOutFp, int size, int begin, int end, char* buffer);
	int writestore(FILE **outFp, Store_Pos *pos_buf, Store_Length *length_buf,
	int obj_no, int start, int end, int group);
	int mergeConj();
	int conjWriteLen;                                  /* byte writen in conjwriteBuffer */
	long conjTotalWrite;                               /* total bytes written in conj.dat */
	char *conjreadBuffer;
	char *conjwriteBuffer;                             /* buffer to write conj.dat */
	int writeConj(FILE *readFp, FILE *writeFp, int length, int group);
	
protected:
	virtual int init();
	virtual int over();
	virtual int brife();
	virtual int store();
	virtual void inpath(char *tp, char* name, int group);
	virtual int writeIndex(int objno, int group, int num);
	virtual int writeDnfIndex(int group, int num);
	
};

#endif
