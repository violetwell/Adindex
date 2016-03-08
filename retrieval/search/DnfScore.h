#ifndef DNFSCORE_H
#define DNFSCORE_H

#include "score.h"
#include "handler.h"
#include <vector>
#include <queue>
#include <algorithm>

class DnfScore:public DocIterator {
private:
	DnfDocIterator* _dScore;
	NoDnfDocIterator* _nDScore;
	int _dnf_nextstat;
	int _dnf_skipstat;
public:
	DnfScore(DnfDocIterator* dscore, NoDnfDocIterator* ndscore, IndexHandler* _handler);
	~DnfScore();
	float score();
	int docId();
	bool next();
	bool skipTo(int target);
};

#endif
