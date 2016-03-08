#ifndef  __TAGS_H_
#define  __TAGS_H_

#include "def.h"
#include "fields.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include <assert.h>
#include <sstream>
#include <iostream>

class Analyzer {
	
public:
	Analyzer(IndexField *_field);
	virtual ~Analyzer();
	int init(char *_data, int _length);
	virtual int analyze()=0;
	TermInfo* getTerms();
	int getTermNum();
	
protected:
	IndexField *field;
	char *data;                                                    /* original data */
	int length;                                                    /* original data size */
	TermInfo *terms;                                               /* store terms extracted from dnf */
	int termNum;                                                   /* count of term in terms */
	
};

class TextAnalyzer : public Analyzer {
	
public:
	TextAnalyzer(IndexField *_field);
	~TextAnalyzer();
	virtual int analyze();
	
private:
	char *emptyPayload;
	
};

class TagsAnalyzer : public Analyzer {
	
public:
	TagsAnalyzer(IndexField *_field);
	~TagsAnalyzer();
	virtual int analyze();
	
};

class DnfAnalyzer : public Analyzer {
	
public:
	DnfAnalyzer(IndexField *_field, char* _levelDbPath);
	~DnfAnalyzer();
	virtual int analyze();
	int getNewConjuctionNum();
	int getOldConjuctionNum();
	int* getNewConjuction();
	int* getOldConjuction();
	int* termConjuction();
	int getMaxConjSize();
	int ready();
	
private:
	char* levelDbPath;
	leveldb::DB* db;
	leveldb::Options options;
	leveldb::Slice *sizeKey;
	
	int cursor;
	char *termDataBuffer;                                /* key(2byte) value(6byte) belong(1byte) */
	int *newConjuction;                                  /* the conjId of new conjunction in dnf */
	int newConjuctionNum;
	int *oldConjuction;                                  /* the conjId of old conjunction in dnf */
	int oldConjuctionNum;
	int *termConjuctionId;                               /* termConjuctionId[i] is the conjId of terms[i] */
	int maxconjsize;
	bool flag;
	
	int preAnalyze();
	bool isConjuction(char* buf, int length);
	int analyzeConjuction(char* buf, int length, int conjuctionId);
	
};

#endif
