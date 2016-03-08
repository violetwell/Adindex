#ifndef TESTSCORE_H
#define TESTSCORE_H

#include "score.h"
#include <vector>
#include <algorithm>

class TestScore:DocIterator
{
private:
	int it;
	size_t index;
	std::vector<std::vector<int> > scorelists;
public:
	TestScore(size_t index, std::vector<std::vector<int> > scorel);
	~TestScore();

	int docId();
	bool next();
	bool skipTo(int target);
	float score();

};

#endif
