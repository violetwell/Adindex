#include "IndexSearch.h"

class SearchQuery;


IndexSearcher::IndexSearcher(IndexHandler* r) {
	handler = r;
}

IndexSearcher::~IndexSearcher() {

}


int32_t IndexSearcher::maxDoc() {
	return 0;
}

void IndexSearcher::_search(SearchQuery* query, Collector* sortedresult) {
    if(NULL == query || NULL == handler) {
		return;
    }

	DocIterator* score;
	score = query->score(handler);

	if(score) {
		while(score->next()) {
			sortedresult->collect(score->docId(), score->score());
		}
	}
}

void IndexSearcher::_search(SearchQuery* query, SimpleTopCollector* results, Collector* sortedresult) {
	struct timeval is_start, is_end, start, end;
	if(NULL == query || NULL == handler) {
		return;
	}

	DocIterator* score;
	score = query->score(results->getMinHeap(), handler);                  /* get doc list of tag query */

	if(score) {
		while(score->next()) {
			results->collect(score->docId(), score->score());              /* collect into min heap which compares score */
		}

	}

	if (NULL != sortedresult) {
		DocIterator* score1;
		score1 = query->score(handler);                                    /* get doc list of dnf and single query */
		if(score1) {
			while(score1->next()) {
				sortedresult->collect(score1->docId(), score1->score());   /* collect into min heap which compares priority field */
			}
		}
	}

	return;
}


IndexHandler* IndexSearcher::gethandler() {
	return handler;
}

