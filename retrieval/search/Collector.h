#ifndef COLLECTOR_H
#define COLLECTOR_H

#include <vector>
#include <map>
#include "MinHeap.h"
#include "Schema.h"
#include "brifereader.h"
#include "math.h"
#include "stdlib.h"

class Collector {
protected:
	size_t _nDocs;
	int32_t _ts;
	float _minScore;
	int _start;
	int _length;
	std::vector<FieldDoc> _rdocs;
	bool _scoreed;
	std::map<int, float> _fdocs;                            /* map<docId, score> */
public:
	Collector(const int start, const int length, bool scoreed);
	~Collector();

	virtual void collect(const int doc, const float score) = 0;
	virtual std::vector<FieldDoc> getResults() = 0;
	virtual std::map<int, float> getFdocs() = 0;
};

//no sort
class NoSortedCollector:public Collector {
public:
	NoSortedCollector(const int start, const int length, bool scoreed);
	~NoSortedCollector();
	
	void collect(const int doc, const float score);
	std::vector<FieldDoc> getResults();
	std::map<int, float> getFdocs();
};

//unstable sort by score
class SimpleTopCollector:public Collector {
private:
	SimpleMinHeap* hq;
public:
	SimpleTopCollector(const int start, const int length, bool scoreed);
	~SimpleTopCollector();	

	MinHeap* getMinHeap();
	void collect(const int doc, const float score);
	std::vector<FieldDoc> getResults();
	std::map<int, float> getFdocs();
};

//with sort policy
class SortedTopCollector:public Collector {
private:
	DiskBrifeReader* _fullbfreader;
	int _fullbase;
	DiskBrifeReader* _incrbfreader;
	int _incrbase;
	RealBrifeReader* _realbfreader;
	int _realbase;
	vector<int> fids;
	vector<int> fls;
	string _sortedby;
	size_t _adpu;
	size_t _adlimit;
	char* lessf;
	SortedIRanMinHeap* hq;
public:
	SortedTopCollector(const int start, const int length, bool scoreed, const size_t adpu, const size_t adlimit, std::string sort, IndexHandler* handler, Schema* schema);
	~SortedTopCollector();
	
	MinHeap* getMinHeap();
	void collect(const int doc, const float score);
	std::vector<FieldDoc> getResults();
	std::map<int, float> getFdocs();
private:
  std::vector<FieldDoc> getResultsWithoutAdpu();
  std::vector<FieldDoc> getResultsWithAdpu();
};

#endif
