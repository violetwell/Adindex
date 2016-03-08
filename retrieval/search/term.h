#ifndef  __TERM_H_
#define  __TERM_H_

#include "def.h"
#include "utils.h"
#include "handler.h"
#include "realtime.h"

class Handler;
class Realtime;

typedef struct Term_Invert {
	long off;                                       /* offset in index.dat */
	int docnum;                                     /* count of doc */
	int invertStart;                                /* for realtime, shift of inverted data in termPostPool */
	int invertUpto;
	float ub;
} TermInvert;

typedef struct DnfTerm_Invert {
	long off;                                       /* offset in dnfindex.dat */
	int conjnum;                                    /* count of conj */
	int invertStart;                                /* for realtime, shift of inverted data in dnftermPostPool */
	int invertUpto;
} DnfTermInvert;

typedef struct Term_Index {
	char *data;
	int dataLen;
	float ub;
	int docnum;
	long indexoff;                                  /* offset in index.dat */
	long termoff;                                   /* offset in term.dat */
} TermIndex;

class TermReader {
	
public:
	TermReader();
	virtual ~TermReader();
	virtual bool term(Term term, TermInvert *invert) = 0;
	virtual bool dnfterm(Term term, DnfTermInvert *invert) = 0;
	
};

/* read inverted data of term,dnfterm in full,incr */
class DiskTermReader : public TermReader {
	
public:
	DiskTermReader(Handler *_handler);
	~DiskTermReader();
	bool term(Term term, TermInvert *invert);
	bool dnfterm(Term term, DnfTermInvert *invert);
	
private:
	Handler *handler;
	char *termidx;
	char *termdat;
	char *dnftermidx;
	char *dnftermdat;
	int termidxlen;
	int termdatlen;
	int dnftermidxlen;
	int dnftermdatlen;
	bool flag;
	TermIndex *termindexs;                                          /* data loaded from term.idx */
	int termindexlen;                                               /* number of term in term.idx */
	int dnftermindexlen;                                            /* number of dnf term in dnfterm.idx */
	char *termbuffer;                                               /* data buffer for termindexs */
	char *cousor;
	int getDnfIndex(Term term);
	int compareTerm(Term term, char* data, int datalen);
	int getIndex(Term term);
	void init();
	int readVInt();
	long readVLong();
	float readUb();
	
};

/* read inverted data of term,dnfterm in realtime */
class RealTermReader : public TermReader {
	
public:
	RealTermReader(Realtime *_realtime);
	~RealTermReader();
	bool term(Term term, TermInvert *invert);
	bool dnfterm(Term term, DnfTermInvert *invert);
	
private:
	Realtime *realtime;
	
};

#endif
