#include "TestScore.h"

int main()
{
	std::vector<int > score;
	std::vector<std::vector<int> > scores;

	score.push_back(1);
	score.push_back(3);

	scores.push_back(score);

	TestScore* ts = new TestScore(0, scores);
	ts->SkipTo(2);

	return 0;
}
