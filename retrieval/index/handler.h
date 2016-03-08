#ifndef  __HANDLER_H_
#define  __HANDLER_H_

#include <vector>
#include "def.h"
#include "term.h"
#include "fields.h"
#include "realtime.h"

using std::vector;

class Realtime;
class TermReader;

class Handler {
	
public:
	Handler(char *_path, IndexSchema *_schema);
	~Handler();
	int init();
	//conj.dat
	unsigned char *ConjDat;
	unsigned int ConjDatSize;
	//conj.idx
	unsigned char *ConjIdx;
	unsigned int ConjIdxSize;
	//dnfindex.dat
	unsigned char *DnfIndexDat;
	unsigned int DnfIndexDatSize;
	//dnfterm.dat
	unsigned char *DnfTermDat;
	unsigned int DnfTermDatSize;
	//dnfterm.idx
	unsigned char *DnfTermIdx;
	unsigned int DnfTermIdxSize;
	//index.dat
	unsigned char *IndexDat;
	unsigned int IndexDatSize;
	//store.len
	unsigned char *StoreLen;
	unsigned int StoreLenSize;
	//store.pos
	unsigned char *StorePos;
	unsigned int StorePosSize;
	//term.dat
	unsigned char *TermDat;
	unsigned int TermDatSize;
	//term.idx
	unsigned char *TermIdx;
	unsigned int TermIdxSize;
	//brifes
	vector<unsigned char*> Brifes;                            /* Brifes[i] store the brife data of ith field in shema.xml */
	//store.dat
	vector<unsigned char*> StoreDat;                          /* store.dat.0 - store.dat.n */
	vector<unsigned int> StoreDatSize;                        /* size of data in StoreDat */
	//del.dat
	unsigned char *DelDat;
	unsigned int DelDatSize;
	//doc num
	int docnum;
	int maxconjsize;
	IndexSchema *schema;
	
private:
	char *path;
	int load(char *name, unsigned char** data, unsigned int* size);
	
};

class IndexHandler {
	
public:
	IndexHandler(Handler *_full, Handler *_incr, Realtime *_realtime);
	~IndexHandler();
	Handler* fullHandler();
	Handler* incrHandler();
	Realtime* realtimeHandler();
	void setFullHandler(Handler *_full);
	void setIncrHandler(Handler *_incr);
	void setRealtimeHandler(Realtime *_realtime);
	TermReader* fullTermReader();
	TermReader* incrTermReader();
	TermReader* realTermReader();
	
private:
	Handler *full;
	Handler *incr;
	Realtime *realtime;
	TermReader *fullReader;
	TermReader *incrReader;
	TermReader *realReader;
	
};

#endif
