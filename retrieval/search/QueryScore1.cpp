#include "QueryScore1.h"
#include <sys/time.h>
#include <queue>

bool cmp(DocIterator* score1, DocIterator* score2) {
	return score1->docId() < score2->docId();
}

/* fq: filter query, tq: single query */
QueryScore1::QueryScore1(char* dnfq, int dnflen, bool dnfex, FilterInfo* fq, QueryInfo* tq, IndexHandler* _handler) : DocIterator(_handler) { 
	//handlers = _handler;
	//printf("dnflen %d dnfex %d\n", dnflen, dnfex);
	//printf("fileter %x\n", fq);
	if (dnflen > 0) {                                                               /* dnf query */
		Dscore = new DnfDocIterator(_handler, dnfq, dnflen, fq);
		NDscore = NULL;
		scores.push_back((DocIterator*) Dscore);
	} else if (dnfex) {
		NoDnfDocIterator* NDscore = new  NoDnfDocIterator(_handler, fq);
		Dscore = NULL;
		scores.push_back((DocIterator*) NDscore);
	} else {
		Dscore = NULL;
		NDscore = NULL;
	}

	if (tq && tq->fieldNum > 0) {                                                    /* single query */
		for (int i(0); i < tq->fieldNum; i++) {
			TermDocIterator* tscore = new TermDocIterator(_handler, tq->fields[i], tq->contents[i], tq->fieldlen[i], fq);
			scores.push_back((DocIterator*) tscore);
		}
	}

	/*
	for(int i(0); i < scores.size(); i++) {
		if(!scores[i]->next())
			docid = INT_MAX;
	}

	sort(scores.begin(), scores.end(), cmp);
	*/

	docid = -1;
	cscore = 0.0;
	
}

QueryScore1::~QueryScore1() {
	for (int i(0); i < scores.size(); i++) {
		delete scores[i];
	}
	scores.clear();
}

int QueryScore1::docId() {
	return docid;
}

float QueryScore1::score() {
	return 0.0;
}

/* find the next doc which is in all doc lists in scores, in order of docId */
bool QueryScore1::next() {

	if (INT_MAX == docid) {
		return false;
	}
	if (0 == scores.size()) {
		docid = INT_MAX;
		return false;
	} else if(1 == scores.size()) {
		if (scores[0]->next()) {
			docid = scores[0]->docId();
			return true;
		} else {
			docid = INT_MAX;
			return false;
		}
	}
	
	for (int i(0); i < scores.size(); i++) {                                  /* since each post list is at current considered doc, skip all post list to next doc */
		if (!scores[i]->next()) {
			docid = INT_MAX;
			return false;
		}
	}

	sort(scores.begin(), scores.end(), cmp);

	if (scores.front()->docId() == scores.back()->docId()) {
		docid = scores.front()->docId();                                      /* we have a doc which is in each post list in scores */
		return true;
	}

	while (scores.front()->skipTo(scores.back()->docId())) {                  /* skip scores[0] to next candidate: docId of last post list */
		DocIterator* score_front = scores.front();                            /* put scores[0] to back of lists */
		scores.erase(scores.begin());

		scores.push_back(score_front);

		if (scores.front()->docId() == scores.back()->docId()) {              /* we have an candidate */
			docid = scores.front()->docId();
			return true;
		}
	}
	docid = INT_MAX;

	return false;
}

bool QueryScore1::skipTo(int target)
{

	if (INT_MAX == docid || (target < 0)) {
		docid = INT_MAX;
		return false;
	}

	if (0 == scores.size()) {
		docid = INT_MAX;
		return false;
	} else if (1 == scores.size()) {

		DocIterator* score0 = scores[0];
		if (score0->skipTo(target)) {
			docid = score0->docId();
			return true;
		} else {
			docid = INT_MAX;
			return false;
		}
	}

	for (int i(0); i < scores.size(); i++) {
		if (!scores[i]->skipTo(target)) {
			docid = INT_MAX;
			return false;
		}
	}
	sort(scores.begin(), scores.end(), cmp);

	if (scores.front()->docId() == scores.back()->docId()) {
		docid = scores.front()->docId();
		return true;
	}

	while (scores.front()->skipTo(scores.back()->docId())) {
        DocIterator* score_front = scores.front();
        scores.erase(scores.begin());

        scores.push_back(score_front);
		
		if (scores.front()->docId() == scores.back()->docId()) {
			docid = scores.front()->docId();
			return true;
		}
	}
	docid = INT_MAX;

	return false;
}
