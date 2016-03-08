#ifndef QUERYSCORE1_H
#define QUERYSCORE1_H

#include "score.h"
#include "handler.h"
//#include <climit>
#include <vector>
#include <queue>
#include <algorithm>
//#include "TestScore.h"

class QueryScore1:public DocIterator {
private:
	std::vector<DocIterator* > scores;
	DnfDocIterator* Dscore;
	NoDnfDocIterator* NDscore;
	//IndexHandler *handlers;
public:
	QueryScore1(char* dnfq, int dnflen, bool dnfex, FilterInfo* fq, QueryInfo* tq, IndexHandler* _handler);
	~QueryScore1();
	float score();
	int docId();
	bool next();
	bool skipTo(int target);

};

#endif
