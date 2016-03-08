#ifndef QUERYSEARCH2_H
#define QUERYSEARCH2_H

#include <vector>

#include "QueryScore1.h"
#include "MinHeap.h"

class QueryScore2:public DocIterator {
private:
	DocIterator* wscore;

public:
	QueryScore2(MinHeap* hq, DocIterator* score, char* sq, int slen, IndexHandler* _handler, FilterInfo* fq);
	~QueryScore2();
	float score();
	int docId();
	bool next();
	bool skipTo(int target);
};

#endif
