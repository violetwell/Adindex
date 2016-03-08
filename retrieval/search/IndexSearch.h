#ifndef INDEXSEARCH_H
#define INDEXSEARCH_H

#include "handler.h"
#include "MinHeap.h"
#include "assert.h"
#include "score.h"
#include "SearchQuery.h"
#include "searchstore.h"
#include "Collector.h"

class IndexSearcher {
private:
	IndexHandler* handler;

public:
	IndexSearcher(IndexHandler* r);

	~IndexSearcher();
	    

	int32_t maxDoc();

	void _search(SearchQuery* query, Collector* sortedresults);
	void _search(SearchQuery* query, SimpleTopCollector* results, Collector* sortedresults);


	IndexHandler* gethandler();

};

#endif
