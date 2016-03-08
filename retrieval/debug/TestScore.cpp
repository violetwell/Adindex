#include "TestScore.h"

TestScore::TestScore(size_t size, std::vector<std::vector<int> > scorel)
{
	scorelists = scorel;
	index = size;
	it = 0;
}

TestScore::~TestScore()
{

}

int TestScore::docId()
{
	if(it == 0)
		return -1;
	std::vector<int > score = scorelists[index];
	return score[it - 1];
}

bool TestScore::next()
{
	it++;
	if((it) > scorelists[index].size())
	{
		return false;
	}
	return true;
}

bool TestScore::skipTo(int target)
{
	if(scorelists[index][it - 1] >= target)
	{
		return true;
	}
	std::vector<int>::iterator result = lower_bound( scorelists[index].begin(), scorelists[index].end(), target);
	std::vector<int>::iterator begin = scorelists[index].begin();
	if(result == scorelists[index].end())
	{
		return false;
	}
	it = result - begin + 1;
	return true;
}

float TestScore::score()
{
	return 0.0;
}
