#include "QueryScore2.h"

/* sq: simiquery, slen: similen */
QueryScore2::QueryScore2(MinHeap* hq, DocIterator* score, char* sq, int slen, IndexHandler* _handler, FilterInfo* fq) : DocIterator(_handler) {
	if (slen == 9 && (NULL ==  score)) {
		TermDocIterator* ws = new TermDocIterator(_handler, sq[0],  (sq+1), 6, fq);
		wscore = (DocIterator*) ws;
	} else if (NULL == score) {
		TagDocIterator* ws = new TagDocIterator(_handler, NULL, sq, slen, hq, fq);
		wscore = (DocIterator*) ws;
	} else {
		TagDocIterator* ws = new TagDocIterator(_handler, score, sq, slen, hq, fq);
		wscore = (DocIterator*) ws;
	}
	
	docid = -1;
	cscore = 0.0;
	
}

QueryScore2::~QueryScore2() {
	delete wscore;
}

float QueryScore2::score() {
	return wscore->score();
}

int QueryScore2::docId() {
	return wscore->docId();
}

bool QueryScore2::next() {
	if (wscore->next()) {
		return true;
	} else {
		docid = INT_MAX;
		return false;
	}

}

bool QueryScore2::skipTo(int target) {
    if (wscore->skipTo(target)) {
		return true;
	} else {
	    docid = INT_MAX;
	    return false;
    }
}

