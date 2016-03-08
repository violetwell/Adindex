#include "Collector.h"

const char * strategy_priority_adpu = "11A";
const char * strategy_priority      = "11B";

Collector::Collector(const int start, const int length, bool scoreed) {
	_start = start;
	_length = length;
	_scoreed = scoreed;
}

Collector::~Collector() {

}

//NoSortedCollector class ~~~~~~~~~~~~~~~~~~~~~~~~~~
NoSortedCollector::NoSortedCollector(const int start, const int length, bool scoreed) : Collector(start, length, scoreed) {
	_nDocs = start + length;
    _ts = 0;
    _minScore = -1.0f;
}

NoSortedCollector::~NoSortedCollector() {
	
}

void NoSortedCollector::collect(const int doc, const float score) {
	if (_rdocs.size() < _nDocs ) {
		//FieldDoc sDoc = {doc, score, 0};
		FieldDoc sDoc = {doc, score, 0, 0};
		_rdocs.push_back(sDoc);
	}
}


std::vector<FieldDoc> NoSortedCollector::getResults() {
	return _rdocs;
}

std::map<int, float> NoSortedCollector::getFdocs() {
	return _fdocs;
}

//SimpleCollector class ~~~~~~~~~~~~~~~~~~~~~~~~~~
SimpleTopCollector::SimpleTopCollector(const int start, const int length, bool scoreed) : Collector(start, length, scoreed) {
	_nDocs = start + length;
    _ts = 0;
	_minScore = -1.0f;
	hq = new SimpleMinHeap(_nDocs);	
}

SimpleTopCollector::~SimpleTopCollector() {
	delete hq;	
}

MinHeap* SimpleTopCollector::getMinHeap() {
	return hq;
}

void SimpleTopCollector::collect(const int doc, const float score) {               /* collect doc into min heap */
    if (!(score < 0.0)) {
		_ts++;
        if (hq->size() < _nDocs || (_minScore==-1.0f || score >= _minScore)) {
            //FieldDoc sd = {doc, score, 0};
        	FieldDoc sd = {doc, score, 0, 0};
            hq->insert(sd);   // update hit queue
            if ( hq->isFull() )
                _minScore = hq->top().score; // maintain minScore
        }
    }
}

/* read _start + _length docs from min heap into _rdocs */
std::vector<FieldDoc> SimpleTopCollector::getResults() {
	std::vector<FieldDoc> tmpdocs;
	int hqsize = hq->size();
	int r_size = 0;
    if (hqsize > 0) {
		for (int i = hqsize; i > 0; i--) {
			FieldDoc ftmp = hq->pop();
			tmpdocs.insert(tmpdocs.begin(), ftmp);                     /* insert from begin, so that doc with higher score comes first */
		}

		if (hqsize >= (_start + _length)) {
			r_size = (_start + _length);
		} else {
			r_size = hqsize;
		}

		if (_scoreed) {
			for (int i = _start; i < r_size; i++) {                    /* only return docs in [_start, r_size] */
				_rdocs.push_back(tmpdocs[i]);
				_fdocs.insert(std::pair<int,float>(tmpdocs[i].doc , tmpdocs[i].score));
			}
		} else {
			for (int i = _start; i < r_size; i++) {
				_rdocs.push_back(tmpdocs[i]);
			}
		}
    }
	return _rdocs;
}

std::map<int, float> SimpleTopCollector::getFdocs() {
	return _fdocs;
}


//SortedTopCollector class ~~~~~~~~~~~~~~~~~~~~~~~~
SortedTopCollector::SortedTopCollector(const int start, const int length, bool scoreed, const size_t adpu, const size_t adlimit, std::string sortedby, IndexHandler* handler, Schema* schema) : Collector(start, length, scoreed) {
    _ts = 0;
	_minScore = -1.0f;
	if ( (start+length)*adlimit > 200)
	{
		_nDocs = 200;
	} else {
		_nDocs = (start+length)*adlimit;
	}
	hq = new SortedIRanMinHeap(_nDocs);
	std::string f1 = "monitorkey";
	std::string f2 = "priority";
	fids.push_back( schema->getFid(f2) );
	fids.push_back( schema->getFid(f1) );
	fls.push_back( schema->getLength(f2) );
	fls.push_back( schema->getLength(f1) );
	lessf = new char[fls[0]];
	memset(lessf, 0, fls[0]);

	_adpu = adpu;
	_adlimit = adlimit;
	_sortedby = sortedby;
	
	if(sortedby == std::string(strategy_priority_adpu) || sortedby == std::string(strategy_priority))                                                                        /* read brife data of "monitorkey", "priority" */
	{
		_fullbase = 0;
		if (NULL != handler->fullHandler()) {
			_incrbase = handler->fullHandler()->docnum;
			_fullbfreader = new DiskBrifeReader(handler->fullHandler(), fids, _fullbase);
		} else {
			_incrbase = 0;
			_fullbfreader = NULL;
		}
		if (NULL != handler->incrHandler()) {
			_realbase = _incrbase + handler->incrHandler()->docnum;
			_incrbfreader = new DiskBrifeReader(handler->incrHandler(), fids, _incrbase);
		} else {
			_realbase = 0;
			_incrbfreader = NULL;
		}
		if (NULL != handler->realtimeHandler()) {
			_realbfreader = new RealBrifeReader(handler->realtimeHandler(), fids, _realbase);
		} else {
			_realbfreader = NULL;
		}
	} else {
		_fullbfreader = NULL;
		_incrbfreader = NULL;
		_realbfreader = NULL;
	}
}

SortedTopCollector::~SortedTopCollector() {

	delete hq;
	
	if(NULL != _fullbfreader) delete _fullbfreader;
	if(NULL != _incrbfreader) delete _incrbfreader;
	if(NULL != _realbfreader) delete _realbfreader;

	if(NULL != lessf) {
		delete [] lessf;
	}
}

MinHeap* SortedTopCollector::getMinHeap() {
	return (MinHeap*) hq;
}

/* read priority field of doc, and collect doc into min heap which compares priority */
void SortedTopCollector::collect(const int doc, const float score) {
    int result;
	memset(lessf, 0, fls[0]);
    if (! (score < 0.0)) {
		_ts++;
		if ( (doc < _incrbase) && (NULL != _fullbfreader) ) {
			char* res = _fullbfreader->getDiskBrife(doc, fids[0], fls[0]);
			if (NULL != res) {
				result = *(int*)res;                                                     /* priority field */
				//FieldDoc fd = {doc, score, result};
				FieldDoc fd = {doc, score, result, hq->ranint()};
				//std::cout << "collect doc: " << fd.doc << " score: " << fd.score << " priority" << fd.fields << std::endl;
				hq->insert(fd);
				}
		} else if( (doc >= _incrbase) && (doc < _realbase) && (NULL != _incrbfreader) ) {
			char* res = _incrbfreader->getDiskBrife(doc, fids[0], fls[0]);
			if (NULL != res) {
				result = *(int*)res;
				//FieldDoc fd = {doc, score, result};
				FieldDoc fd = {doc, score, result, hq->ranint()};
				//std::cout << "collect doc: " << fd.doc << " score: " << fd.score << " priority" << fd.fields << std::endl;
				hq->insert(fd);
			}
		} else if( (doc >= _realbase) && (NULL != _realbfreader) ) {
			if (_realbfreader->getRealBrife(doc, fids[0], lessf, fls[0])) {
				result = *(int*)lessf;
				//FieldDoc fd = {doc, score, result};
				FieldDoc fd = {doc, score, result, hq->ranint()};
				//std::cout << "collect doc: " << fd.doc << " score: " << fd.score << " priority" << fd.fields << std::endl;
				hq->insert(fd);
			}
		}
    }
}

/* read _start + _length docs from min heap into _rdocs, considering adpu */
std::vector<FieldDoc> SortedTopCollector::getResultsWithAdpu() {
	std::map<int64_t, int > adlimits;
	std::vector<FieldDoc> tmpdocs;
	std::vector<FieldDoc> r_docs;
	int iLen = 4;
	char* monitkey = new char[fls[1]];
	memset(monitkey, 0, fls[1]);
	std::map<int64_t, int>::iterator it;
	int hqsize = hq->size();
	if(hqsize > 0) {
		for (int i = hqsize; i > 0; i--) {
			FieldDoc ftmp = hq->pop();
			//std::cout << "get tmp doc: " << ftmp.doc << " score: " << ftmp.score << " priority" << ftmp.fields << std::endl;
			tmpdocs.push_back(ftmp);
		}

		for (int x = tmpdocs.size(); x > 0 ; x--) {                            /* reversely look into tmpdocs, doc with higher priority comes first */
			memset(monitkey, 0, fls[1]);			
			int64_t aduid(-1);
			FieldDoc fDoc = tmpdocs[x - 1];
			//std::cout << "get res doc: " << fDoc.doc << " score: " << fDoc.score << " priority" << fDoc.fields << std::endl;
			if ( (fDoc.doc < _incrbase) && (NULL != _fullbfreader) ) {
				char* res = _fullbfreader->getDiskBrife(fDoc.doc, fids[1], fls[1]);                /* read monitor key */
				if (NULL != res) {
					for(int j(1); j < (iLen+1); j++) {
						int64_t tmp=0;
						if((res[j]>=48)&&(res[j]<=57)) {
							int64_t t=res[j]-48;
							tmp=pow(62,(iLen-j))*t;
						}
						if((res[j]>=97)&&(res[j]<=122)) {
							int64_t t=res[j]-87;
							tmp=pow(62,(iLen-j))*t;
						}
						if ((res[j]>=65)&&(res[j]<=90)) {
							int64_t t=res[j]-29;
							tmp=pow(62,(iLen-j))*t;
						}
						aduid+=tmp;
					}
				}
			} else if ( (fDoc.doc >= _incrbase) && (fDoc.doc < _realbase) && (NULL != _incrbfreader) ) {
				char* res = _incrbfreader->getDiskBrife(fDoc.doc, fids[1], fls[1]);
				if(NULL != res) {
					for(int j(1); j < (iLen+1); j++) {
						int64_t tmp=0;
						if((res[j]>=48)&&(res[j]<=57)) {
							int64_t t=res[j]-48;
							tmp=pow(62,(iLen-j))*t;
						}
						if((res[j]>=97)&&(res[j]<=122)) {
							int64_t t=res[j]-87;
							tmp=pow(62,(iLen-j))*t;
						}
						if((res[j]>=65)&&(res[j]<=90)) {
							int64_t t=res[j]-29;
							tmp=pow(62,(iLen-j))*t;
						}
						aduid+=tmp;
					}			
				}
			} else if ( (fDoc.doc >= _realbase) && (NULL != _realbfreader) ) {
				if (_realbfreader->getRealBrife(fDoc.doc, fids[1], monitkey, fls[1])) {
					for(int j(1); j < (iLen+1); j++) {
						int64_t tmp=0;
						if((monitkey[j]>=48)&&(monitkey[j]<=57)) {
							int64_t t=monitkey[j]-48;
							tmp=pow(62,(iLen-j))*t;
						}
						if((monitkey[j]>=97)&&(monitkey[j]<=122)) {
							int64_t t=monitkey[j]-87;
							tmp=pow(62,(iLen-j))*t;
						}
						if((monitkey[j]>=65)&&(monitkey[j]<=90)) { 
							int64_t t=monitkey[j]-29;
							tmp=pow(62,(iLen-j))*t; 
						}
						aduid+=tmp;
					}
				}
			}

			if(aduid != -1) {
				aduid++;
				it = adlimits.find(aduid);
				if(adlimits.end() != it) {
					if(adlimits[aduid] < _adpu) {                                           /* less than adpu, OK */
						if ( _scoreed ) {
							//r_docs.insert(r_docs.begin(), fDoc);
							r_docs.push_back(fDoc);
							adlimits[aduid]++;                                              /* increase count */
							_fdocs.insert(std::pair<int,float>(fDoc.doc , fDoc.score));
						} else {
							//r_docs.insert(r_docs.begin(), fDoc);
							r_docs.push_back(fDoc);
							adlimits[aduid]++;
						}
					}
				} else {
					if (_scoreed ) {
						//r_docs.insert(r_docs.begin(), fDoc);
						r_docs.push_back(fDoc);
						adlimits[aduid] = 1;
						_fdocs.insert(std::pair<int,float>(fDoc.doc , fDoc.score));
					} else {
						//r_docs.insert(r_docs.begin(), fDoc);
						r_docs.push_back(fDoc);
						adlimits[aduid] = 1;
					}
				}
			}
		}
	}
	if(NULL != monitkey)
		delete [] monitkey;
	
	int r_size(0);

	if ( r_docs.size() >= (_start + _length) ) {
		r_size = (_start + _length);
	} else {
		r_size = r_docs.size();
	}
	for(int i(_start); i < r_size; i++) {
		//_rdocs.insert(_rdocs.begin(), r_docs[i]);
		_rdocs.push_back(r_docs[i]);
	}
	return _rdocs;
}

/* read _start + _length docs from min heap into _rdocs */
std::vector<FieldDoc> SortedTopCollector::getResultsWithoutAdpu() {
	std::vector<FieldDoc> tmpdocs;
	std::vector<FieldDoc> r_docs;
	int hqsize = hq->size();
	
	if(hqsize > 0) {
		for (int i = hqsize; i > 0; i--) {
			FieldDoc ftmp = hq->pop();
			//std::cout << "get tmp doc: " << ftmp.doc << " score: " << ftmp.score << " priority" << ftmp.fields << std::endl;
			tmpdocs.push_back(ftmp);
		}

		for (int x = tmpdocs.size(); x > 0 ; x--) {                            /* reversely look into tmpdocs, doc with higher priority comes first */
			FieldDoc fDoc = tmpdocs[x - 1];
      
      r_docs.push_back(fDoc);
			if (_scoreed )			
				_fdocs.insert(std::pair<int,float>(fDoc.doc , fDoc.score));
		}
	}
	
	int r_size(0);

	if ( r_docs.size() >= (_start + _length) ) {
		r_size = (_start + _length);
	} else {
		r_size = r_docs.size();
	}
	for(int i(_start); i < r_size; i++) {
		//_rdocs.insert(_rdocs.begin(), r_docs[i]);
		_rdocs.push_back(r_docs[i]);
	}
	return _rdocs;
}

std::vector<FieldDoc> SortedTopCollector::getResults() 
{
    if(_sortedby == std::string(strategy_priority))
        return getResultsWithoutAdpu();
    else
        return getResultsWithAdpu();
}

std::map<int, float> SortedTopCollector::getFdocs() {
	return _fdocs;
}
