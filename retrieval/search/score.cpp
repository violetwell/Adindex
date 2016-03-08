#include "score.h"

#include <log4cxx/logger.h>
#include <log4cxx/logstring.h>
#include <log4cxx/propertyconfigurator.h>
#include <stdarg.h>
#include <string>

using namespace log4cxx;

//LoggerPtr searchLog(Logger::getLogger("searchsev"));
extern LoggerPtr searchLog;

DocIterator::DocIterator(IndexHandler *_handlers) {
	handlers = _handlers;
}

DocIterator::~DocIterator() {
}

NoDnfDocIterator::NoDnfDocIterator(IndexHandler *_handlers, FilterInfo * _filterInfo) 
: DocIterator(_handlers) {
	docid = -1;
	cscore = 0.0;
	state = 0;
	filterInfo = _filterInfo;
	docs = (ConjDocs**)malloc(sizeof(ConjDocs*) * 3);
	filters = (Filter**)malloc(sizeof(Filter*) * 3);
	bzero(docs, sizeof(ConjDocs*) * 3);
	bzero(filters, sizeof(Filter*) * 3);
	combine = NULL;
	init();
}

NoDnfDocIterator::~NoDnfDocIterator() {
	if (docs != NULL) {
		int i = 0;
		for (i = 0; i < 3; i++) {
			ConjDocs* conj = docs[i];
			if (conj != NULL) {
				delete conj;
				docs[i] = NULL;
			}
		}
		free(docs);
		docs = NULL;
	}
	if (filters != NULL) {
		int i = 0;
		for (i = 0; i < 3; i++) {
			Filter * filter = filters[i];
			if (filter != NULL) {
				delete filter;
				filters[i] = NULL;
			}
		}
		free(filters);
		filters = NULL;
	}
	if (combine != NULL) {
		delete combine;
		combine = NULL;
	}
}

int NoDnfDocIterator::docId() {
	return docid;
}

bool NoDnfDocIterator::next() {
	if (state == -1) return false;
	bool res = combine->next();
	if (!res) {
		docid = -1;
		state = -1;
	} else docid = combine->docid;
	return res;
}

bool NoDnfDocIterator::skipTo(int target) {
	if (state == -1) return false;
	int cur = combine->skipTo(target);
	if (cur == INT_MAX) {
		docid = -1;
		state = -1;
		return false;
	} else {
		docid = cur;
	    return true;
	}
}

float NoDnfDocIterator::score() {
	return cscore;
}

/* search docIds of which dnf field is empty, and combine docId lists into combine */
void NoDnfDocIterator::init() {
	if (handlers == NULL) {
		state = -1;
		return;
	}
	char *cousor = NULL;
	char *delFile = NULL;
	int base = 0;
	int couter = 0;
	Handler *full = handlers->fullHandler();
	Handler *incr = handlers->incrHandler();
	Realtime *realtime = handlers->realtimeHandler();
	if (NULL != filterInfo) {
		if (NULL != full) filters[0] = new DiskFilter(full, filterInfo);
		else filters[0] = NULL;
		if (NULL != incr) filters[1] = new DiskFilter(incr, filterInfo);
		else filters[1] = NULL;
		if (NULL != realtime) filters[2] = new RealFilter(realtime, filterInfo);
		else filters[2] = NULL;
	} else {
		for (int i = 0; i < 3; i++) {
			filters[i] = NULL;
		}
	}
	if (full != NULL) {                                                          /* docIds in full */
		cousor = (char*)full->ConjIdx;
		delFile = (char*)full->DelDat;
		long offset = *((long*)cousor);
		cousor += 8;
		int docnum = *((int*)cousor);
		if (offset == 0 && docnum > 0) {
			cousor = (char*)full->ConjDat;
			docs[couter++] = new DiskConjDocs(base, cousor + offset, delFile, docnum, filters[0]);
		}
		base += full->docnum;
	}
	if (incr != NULL) {                                                          /* docIds in incr */
		cousor = (char*)incr->ConjIdx;
		delFile = (char*)incr->DelDat;
		long offset = *((long*)cousor);
		cousor += 8;
		int docnum = *((int*)cousor);
		if (offset == 0 && docnum > 0) {
			cousor = (char*)incr->ConjDat;
			docs[couter++] = new DiskConjDocs(base, cousor + offset, delFile, docnum, filters[1]);
		}
		base += incr->docnum;
	}
	if (realtime != NULL && realtime->getConjnum() > 1) {                        /* docIds in realtime */
		int *data = realtime->dnfconjPostIndex->buffers[0];
		int offset = 0;
		if (data[offset] < data[offset + 1]) {
		    RtByteSliceReader *rtreader = new RtByteSliceReader();
			rtreader->init(realtime->dnfconjPostPool, data[offset], data[offset + 1]);
		    docs[couter++] = new RealConjDocs(base, realtime->getDocnum(), rtreader, realtime,filters[2]);
		}
	}
	if (couter == 0) state = -1;
	else {
		combine = new Combine(docs, couter);
	}
}

/* _field: fieldId, _data: fieldData */
TermDocIterator::TermDocIterator(IndexHandler *_handlers, char _field, char *_data, int _dataLen, FilterInfo * _filterInfo) 
: DocIterator(_handlers) {
	docid = -1;
	cscore = 0.0;
	state = 0;
	muti = NULL;
	filterInfo = _filterInfo;
	docs = (TermDocs**)malloc(sizeof(TermDocs*) * 3);
	filters = (Filter**)malloc(sizeof(Filter*) * 3);
	bzero(docs, sizeof(TermDocs*) * 3);
	bzero(filters, sizeof(Filter*) * 3);
	term.field = _field;                                                /* term to search */
	term.data = (unsigned char*)_data;
	term.dataLen = _dataLen;
	init();
}

TermDocIterator::~TermDocIterator() {
	if (muti != NULL) {
		delete muti;
		muti = NULL;
	} else {
		if (NULL != docs) {
			free(docs);
			docs = NULL;
		}
	}
	if (filters != NULL) {
		int i = 0;
		for (; i < 3; i++) {
			if (filters[i] != NULL) {
				delete filters[i];
				filters[i] = NULL;
			}
		}
		free(filters);
		filters = NULL;
	}
}

int TermDocIterator::docId() {
	return docid;
}

bool TermDocIterator::next() {
	if (state == -1) return false;
	bool res = muti->next();
	if (!res) {
		docid = -1;
		state = -1;
	} else docid = muti->docid; 
	return res;
}

bool TermDocIterator::skipTo(int target) {
	if (state == -1) return false;
	int cur = muti->skipTo(target);
	if (cur == INT_MAX) {
		docid = -1;
		state = -1;
		return false;
	} else {
		docid = cur;
	    return true;
	}
}

float TermDocIterator::score() {
	return cscore;
}

void TermDocIterator::init() {
	if (handlers == NULL || term.data == NULL || term.dataLen <= 0) {
		state = -1;
		return;
	}
	int couter = 0;
	int base = 0;
	TermInvert invert;
	bool res = true;
	TermReader *reader = NULL;
	char *cousor = NULL;
	char *delFile = NULL;
	Handler *full = handlers->fullHandler();
	Handler *incr = handlers->incrHandler();
	Realtime *realtime = handlers->realtimeHandler();
	if (NULL != filterInfo) {                                                           /* filter */
		if (NULL != full) filters[0] = new DiskFilter(full, filterInfo);
		else filters[0] = NULL;
		if (NULL != incr) filters[1] = new DiskFilter(incr, filterInfo);
		else filters[1] = NULL;
		if (NULL != realtime) filters[2] = new RealFilter(realtime, filterInfo);
		else filters[2] = NULL;
	} else {
		for (int i= 0; i < 3; i++) {
			filters[i] = NULL;
		}
	}
	if (full != NULL) {
		reader = handlers->fullTermReader();
		res = reader->term(term, &invert);                                              /* find inverted data of term */
		if (res && ((int)invert.off < (int)full->IndexDatSize)) {
			cousor = (char*)full->IndexDat;
			delFile = (char*)full->DelDat;
			docs[couter++] = new DiskTermDocs(0.0f, base, cousor + (int)invert.off, delFile, invert.docnum, filters[0]);
		}
		base += full->docnum;
	}
	if (incr != NULL) {
		reader = handlers->incrTermReader();
		res = reader->term(term, &invert);
		if (res && ((int)invert.off < (int)incr->IndexDatSize)) {
			cousor = (char*)incr->IndexDat;
			delFile = (char*)incr->DelDat;
			docs[couter++] = new DiskTermDocs(0.0f, base, cousor + (int)invert.off, delFile, invert.docnum, filters[1]);
		}
		base += incr->docnum;
	}
	if (realtime != NULL) {
		reader = handlers->realTermReader();
		res = reader->term(term, &invert);
		if (res && (invert.invertStart != -1)) {
			RtByteSliceReader *rtreader = new RtByteSliceReader();
			rtreader->init(realtime->termPostPool, invert.invertStart, invert.invertUpto);
			docs[couter++] = new RealTermDocs(0.0f, base, realtime->getDocnum(), rtreader, realtime, filters[2]);
		}
	}
	if (couter == 0) state = -1;
	else {
		muti = new MutiTermDocs(docs, couter);                                            /* store into muti */
	}
}

/* _data: dnf query */
DnfDocIterator::DnfDocIterator(IndexHandler *_handlers, char *_data, int _dataLen, FilterInfo * _filterInfo) : DocIterator(_handlers) {
	docid = -1;
	cscore = 0.0f;
	data = _data;
	dataLen = _dataLen;
	filterInfo = _filterInfo;
	filters = (Filter **)malloc(sizeof(Filter*) * 3);
	bzero(filters, sizeof(Filter*) * 3);
	conjsize = 0;
	state = 0;
	zeroterm = NULL;
	terms = NULL;
	subnums = NULL;
	conjdocs = NULL;
	combine = NULL;
	conjdocnum = 0;
	init();
}

DnfDocIterator::~DnfDocIterator() {
	if (zeroterm != NULL) {
		free(zeroterm);
		zeroterm = NULL;
	}
	if (terms != NULL) {
		free(terms);
		terms = NULL;
	}
	if (subnums != NULL) {
		free(subnums);
		subnums = NULL;
	}
	if (combine != NULL) {
		delete combine;
		combine = NULL;
	}
	if (conjdocs != NULL) {
		for (int i = 0; i < conjdocnum; i++) {
			ConjDocs *conjdoc = conjdocs[i];
			if (conjdoc != NULL) {
				delete conjdoc;
				conjdocs[i] = NULL;
			}
		}
		free(conjdocs);
		conjdocs = NULL;
	}

	if (filters != NULL) {
		for (int i = 0; i < 3; i++) {
			Filter * filter = filters[i];
			if (filter != NULL) {
				delete filter;
				filters[i] = NULL;
			}
		}
		free(filters);
		filters = NULL;
	}
}

int DnfDocIterator::docId() {
	return docid;
}

bool DnfDocIterator::next() {
	if (state == -1) {
		return false;
	}
	bool res = combine->next();
	if (!res) {
		docid = -1;
		state = -1;
	} else docid = combine->docid;
	return res;
}

bool DnfDocIterator::skipTo(int target) {
	if (state == -1) return false;
	int cur = combine->skipTo(target);
	if (cur == INT_MAX) {
		docid = -1;
		state = -1;
		return false;
	} else {
		docid = cur;
	    return true;
	}
}

float DnfDocIterator::score() {
	return cscore;
}

/* search conjIds which fits dnf-query, then search docIds which fits filter and not deleted, then merge docId lists into combine */
void DnfDocIterator::init() {
	if (handlers == NULL || data == NULL 
	|| dataLen <= 0 || ((dataLen % 9) != 0) || data[8] != 0) {
		state = -1;
		return;
	}
	zeroterm = (char*)malloc(sizeof(char) * 8);
	bzero(zeroterm, sizeof(char) * 8);
	zero.field = 0;
	zero.data = (unsigned char*)zeroterm;
	zero.dataLen = 8;
	int tagnum = dataLen / 9;
	terms = (Term*)malloc(sizeof(Term) * tagnum);
	bzero(terms, sizeof(Term) * tagnum);
	subnums = (int*)malloc(sizeof(int) * (tagnum + 1));               // 最后一个留给 Z term
	bzero(subnums, sizeof(int) * (tagnum + 1));
	int i = 0, lasti = 0;
	for (; i < tagnum; i++) {                                         /* load dnf query into terms, subnums, conjsize */
		terms[i].data = (unsigned char*)data + (9 * i);
		terms[i].dataLen = 8;
		if ((data[9 * i + 8] == 0)) {                                 /* DIFF FIELD */
			if (i > lasti) {
				subnums[conjsize++] = i - lasti;
			}
			lasti = i;
		}
	}
	subnums[conjsize++] = i - lasti;
	
	DnfTermInvert invert;
	bool res = true;
	TermReader *reader = NULL;
	char *cousor = NULL;
	Handler *full = handlers->fullHandler();
	Handler *incr = handlers->incrHandler();         
	Realtime *realtime = handlers->realtimeHandler();
	if (NULL != filterInfo) {
		if (NULL != full) filters[0] = new DiskFilter(full, filterInfo);
		else filters[0] = NULL;
		if (NULL != incr) filters[1] = new DiskFilter(incr, filterInfo);
		else filters[1] = NULL;
		if (NULL != realtime) filters[2] = new RealFilter(realtime, filterInfo);
		else filters[2] = NULL;
	} else {
		for (int i = 0; i < 3; i++) {
			filters[i] = NULL;
		}
	}
	if (full != NULL) {                                                                       /* search conjIds in full */
		int realsize = full->maxconjsize < conjsize ? full->maxconjsize : conjsize;
		cousor = (char*)full->DnfIndexDat;
		reader = handlers->fullTermReader();
		DnfTermDocs **streams = (DnfTermDocs**)malloc(sizeof(DnfTermDocs*) * (tagnum + 1));   /* post list of dnf-fields, might contain Z */
		int *subListNums = (int*)malloc(sizeof(int) * (tagnum + 1)); 
		for (int level = 0; level <= realsize; level++) {                                         /* iterate each size-level of conjunction */
		    int n = 0, m = 0, combinnum = 0, skip = 0, index = 0, docCounter = 0;
			  for (n = 0; n < tagnum; n++) terms[n].field = level;
		    bzero(streams, sizeof(DnfTermDocs*) * (tagnum + 1));
	      memcpy(subListNums, subnums, sizeof(int) * (tagnum + 1));   
		    for (n = 0; n < conjsize; n++) {                                                      /* for each dnf-fields, combine post list of different assignments into one in combins */
			      skip = subListNums[n];
			      for (m = 0; m < skip; m++) {
					      res = reader->dnfterm(terms[index++], &invert);
				        if (res && invert.conjnum > 0 && ((int)invert.off < (int)full->DnfIndexDatSize)) {
						        streams[docCounter++] = new DiskDnfTermDocs(cousor + (int)invert.off, invert.conjnum);
					      } else {
					          -- subListNums[n];
					      }
			      }
				    if (subListNums[n] > 0) {
					      combinnum++;
				    }
		    }
			if (level == 0) {                                                                      /* Z term */
				res = reader->dnfterm(zero, &invert);
				if (res && invert.conjnum > 0 && ((int)invert.off < (int)full->DnfIndexDatSize)) {
					streams[docCounter++] = new DiskDnfTermDocs(cousor + (int)invert.off, invert.conjnum);
					subListNums[conjsize] = 1;
					combinnum++;
				}
			}
			if (combinnum >= ((level == 0) ? 1 : level)) {                                         /* count of post list should be no less than level */
				  queryconj(streams, subListNums, conjsize + 1, level, 0);                                  /* find conjIds in this level */
			}
			for (n = 0; n < tagnum + 1; n++) {
				if (streams[n] != NULL) {
					delete streams[n];
					streams[n] = NULL;
				}
			}
		}
		free(streams);
	  free(subListNums);
	}
	if (incr != NULL) {                                                                            /* search conjIds in incr */
		int realsize = incr->maxconjsize < conjsize ? incr->maxconjsize : conjsize;
		cousor = (char*)incr->DnfIndexDat;
		reader = handlers->incrTermReader();
		DnfTermDocs **streams = (DnfTermDocs**)malloc(sizeof(DnfTermDocs*) * (tagnum + 1));   /* post list of dnf-fields, might contain Z */
		int *subListNums = (int*)malloc(sizeof(int) * (tagnum + 1)); 
		for (int level = 0; level <= realsize; level++) {
		    int n = 0, m = 0, combinnum = 0, skip = 0, index = 0, docCounter = 0;
			  for (n = 0; n < tagnum; n++) terms[n].field = level;
		    bzero(streams, sizeof(DnfTermDocs*) * (tagnum + 1));
	      memcpy(subListNums, subnums, sizeof(int) * (tagnum + 1)); 
		    for (n = 0; n < conjsize; n++) {
			    skip = subListNums[n];
			    for (m = 0; m < skip; m++) {
					  res = reader->dnfterm(terms[index++], &invert);
				    if (res && invert.conjnum > 0 && ((int)invert.off < (int)incr->DnfIndexDatSize)) {
						  streams[docCounter++] = new DiskDnfTermDocs(cousor + (int)invert.off, invert.conjnum);
					  } else {
					    -- subListNums[n];    
					  }
			    }
				  if (subListNums[n] > 0) {
					      combinnum++;
				  }
		    }
			if (level == 0) {
				res = reader->dnfterm(zero, &invert);
				if (res && invert.conjnum > 0 && ((int)invert.off < (int)incr->DnfIndexDatSize)) {
					streams[docCounter++] = new DiskDnfTermDocs(cousor + (int)invert.off, invert.conjnum);
					subListNums[conjsize] = 1;
					combinnum++;
				}
			}
			if (combinnum >= ((level == 0) ? 1 : level)) {
				queryconj(streams, subListNums, conjsize + 1, level, 1); 
			}
			for (n = 0; n < tagnum + 1; n++) {
				if (streams[n] != NULL) {
					delete streams[n];
					streams[n] = NULL;
				}
			}
		}
		free(streams);
		free(subListNums);		
	}
	int realtimemax = 0, realmaxdoc = 0, realmaxconj = 0;
	if (realtime != NULL) {                                                                           /* search conjIds in realtime */	   
		realtimemax = realtime->getMaxConjSize();
		realmaxdoc = realtime->getDocnum();
		realmaxconj = realtime->getConjnum();
		reader = handlers->realTermReader();
		int realsize = realtimemax < conjsize ? realtimemax : conjsize;
		DnfTermDocs **streams = (DnfTermDocs**)malloc(sizeof(DnfTermDocs*) * (tagnum + 1));   /* post list of dnf-fields, might contain Z */
		int *subListNums = (int*)malloc(sizeof(int) * (tagnum + 1)); 
		for (int level = 0; level <= realsize; level++) {
		    int n = 0, m = 0, combinnum = 0, skip = 0, index = 0, docCounter = 0;
			  for (n = 0; n < tagnum; n++) terms[n].field = level;
		    bzero(streams, sizeof(DnfTermDocs*) * (tagnum + 1));
	      memcpy(subListNums, subnums, sizeof(int) * (tagnum + 1));  
		    for (n = 0; n < conjsize; n++) {
			    skip = subListNums[n];
			    for (m = 0; m < skip; m++) {
					  res = reader->dnfterm(terms[index++], &invert);
				    if (res && (invert.invertStart != -1)) {
						RtByteSliceReader *rtreader = new RtByteSliceReader();
						rtreader->init(realtime->dnftermPostPool, invert.invertStart, invert.invertUpto);
						streams[docCounter++] = new RealDnfTermDocs(realmaxconj, rtreader);
					  } else {
					      -- subListNums[n];
					  }
			    }
				  if (subListNums[n] > 0) {
					    combinnum++;
				  }
		    }
			if (level == 0) {
				res = reader->dnfterm(zero, &invert);
				if (res && (invert.invertStart != -1)) {
					RtByteSliceReader *rtreader = new RtByteSliceReader();
					rtreader->init(realtime->dnftermPostPool, invert.invertStart, invert.invertUpto);
					streams[docCounter++] = new RealDnfTermDocs(realmaxconj, rtreader);
					subListNums[conjsize] = 1;
					combinnum++;
				}
			}
			if (combinnum >= ((level == 0) ? 1 : level)) {
				queryconj(streams, subListNums, conjsize + 1, level, 2);
			}
			for (n = 0; n < tagnum + 1; n++) {
				if (streams[n] != NULL) {
					delete streams[n];
					streams[n] = NULL;
				}
			}
		}
		free(streams);
		free(subListNums);
	}
	if (conjs.size() > 0) {                                                                  /* get docIds */
		char * delFile = NULL;
		conjdocs = (ConjDocs**)malloc(sizeof(ConjDocs*) * conjs.size());
		bzero(conjdocs, sizeof(ConjDocs*) * conjs.size());
		for (i = 0; i < (int)conjs.size(); i++) {
			Pair conjpair = conjs[i];
			if (conjpair.type == 0) {                                                        /* full */
				cousor = (char*)full->ConjIdx + 12 * conjpair.conjId;
				long offset = *((long*)cousor);
				cousor += 8;
				int docnum = *((int*)cousor);
				if (docnum > 0) {
					cousor = (char*)full->ConjDat;
					delFile = (char*)full->DelDat;
					conjdocs[conjdocnum++] = new DiskConjDocs(0, cousor + offset, delFile, docnum, filters[0]);
				}
			} else if (conjpair.type == 1) {                                                 /* incr */
				cousor = (char*)incr->ConjIdx + 12 * conjpair.conjId;
				long offset = *((long*)cousor);
				cousor += 8;
				int docnum = *((int*)cousor);
				if (docnum > 0) {
					cousor = (char*)incr->ConjDat;
					delFile = (char*)incr->DelDat;
					conjdocs[conjdocnum++] = new DiskConjDocs(full != NULL ? full->docnum : 0, cousor + offset, delFile, docnum, filters[1]);
				}
			} else if (conjpair.type == 2) {                                                 /* realtime */
				if (conjpair.conjId >= realmaxconj) continue;
				int base = full != NULL ? full->docnum : 0;
				base += incr != NULL ? incr->docnum : 0;
				int index = conjpair.conjId * 2;
				int *data = realtime->dnfconjPostIndex->buffers[index >> INT_BLOCK_SHIFT];
				int offset = index & INT_BLOCK_MASK;
				if (data[offset + 1] > 0 && data[offset + 1] > data[offset]) {
				    RtByteSliceReader *rtreader = new RtByteSliceReader();
				    rtreader->init(realtime->dnfconjPostPool, data[offset], data[offset + 1]);
					conjdocs[conjdocnum++] = new RealConjDocs(base, realmaxdoc, rtreader, realtime, filters[2]);
				}
			}
		}
		if (conjdocnum > 0) {
			combine = new Combine(conjdocs, conjdocnum);                                     /* merge docId list into combine */
		} else {
			state = -1;
		}
	} else {
		state = -1;
	}
}

/* find conjIds which fits dnf query in size-level:level */
void DnfDocIterator::queryconj(DnfTermDocs **docs, int *subNums, int subNumSize, int level, int type)
{
    level = (level == 0) ? 1 : level;
        
    int listNum = subNumSize;
    int *subNumLeft = new int[subNumSize];
    for (int i = 0; i < subNumSize; ++ i) {
        subNumLeft[i] = subNums[i];
    }

    bool listEnded = false;
    int docListIdx = 0;
    for (int index = 0; index < subNumSize; ++index) {
        for (int innerIndex = 0; innerIndex < subNums[index]; ++innerIndex) {
            if (!docs[docListIdx]->next()) {
                -- subNumLeft[index];
                //LOG4CXX_INFO(searchLog, "list ended: " << index << "-" << innerIndex);
            }
            ++ docListIdx;
        }
        if (subNumLeft[index] == 0) {
            //LOG4CXX_INFO(searchLog,"-- listNum for " << index << "th term");
            -- listNum;
        }  
    }
    if (listNum < level) {
        listEnded = true;
    }
    
    int hitNumMap[1024];
    bool belongMap[1024];
    char markMap[1024/8];                              // 标记同一个 dnfterm 在 hitNumMap 上的加成动作
    int minId = INT_MAX, start = 0, end = 1024;
  
    while(!listEnded) {
        //LOG4CXX_INFO(searchLog, "start: " << start << ", end: " << end);
        docListIdx = 0;
        bzero(hitNumMap, 1024 * sizeof(int));
        memset(belongMap,1,sizeof(belongMap));
        for (int index = 0; index < subNumSize; ++index) {
            bzero(markMap, 1024/8);
            for (int innerIndex = 0; innerIndex < subNums[index]; ++innerIndex) {
                while(true) {
                    int conjId = docs[docListIdx]->conjid;
                    bool belong = docs[docListIdx]->isbelong;
                    //LOG4CXX_INFO(searchLog, docListIdx << "th doc, conjId: " << docs[docListIdx]->conjid << ", belong: " << docs[docListIdx]->isbelong); 
                    if (conjId == INT_MAX || conjId >= end) {
                        minId = minId < conjId ? minId: conjId;         // 记录 minId
                        break;    
                    }
                    if (!belong) {
                        belongMap[conjId - start] = belong;
                    }
                    if (!(markMap[(conjId - start)>>3] & ((char)1 << (conjId - start)%8))) {   // 本 dnf term 没有命中过
                        ++ hitNumMap[conjId - start];
                        markMap[(conjId - start)>>3] |= (char)1 << (conjId - start)%8;
                        //LOG4CXX_INFO(searchLog, "increase hit num for conjId: " << conjId);
                    }
                    if (!docs[docListIdx]->next()) {                                           // 链转空，跳过此链
                        //LOG4CXX_INFO(searchLog, "list ended: " << index << "-" << innerIndex);
                        -- subNumLeft[index];
                        if (subNumLeft[index] == 0) {                                         // 某个 dnf term 的倒排链转空
                            -- listNum;
                            //LOG4CXX_INFO(searchLog, "-- listNum for " << index << "th term");
                        }    
                        break;
                    }
                }
                ++ docListIdx;
            }
        }
        
        if (listNum < level) {
            listEnded = true;
            //LOG4CXX_INFO(searchLog, "listEnded");
        }
        
        for (int i = 0; i < 1024; ++ i) {
            //if (hitNumMap[i]) {
                //LOG4CXX_INFO(searchLog, "level: " << level << ", conjId: " << i + start << ", hit num: " << hitNumMap[i] << ", belong: " << belongMap[i]);    
            //}
            if ((hitNumMap[i] == level) && belongMap[i]) {
                //LOG4CXX_INFO(searchLog, "get conjId: " << i + start);                                     // 获得 conjId
                Pair pr;
					      pr.conjId = i + start;
					      pr.type = type;
		    		    conjs.push_back(pr);                                                           // store into conjs
            }    
        }
        
        start = minId;
        end = start + 1024;
        minId = INT_MAX;
    } 
    delete [] subNumLeft;   
}

int DnfDocIterator::compareTermDocs(InnerCombine *p1, InnerCombine *p2) {
	if (p1->curdoc == p2->curdoc) {
		if (p1->curbelong == p2->curbelong) return 0;
		else if (!p1->curbelong) return -1;
		else return 1;
	} else return p1->curdoc - p2->curdoc;
}

/* sort in increasing order of current docId and belong */
void DnfDocIterator::quickSort(InnerCombine **termDocs, int lo, int hi) {
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (compareTermDocs(termDocs[lo], termDocs[hi]) > 0) {
			InnerCombine *tmp = termDocs[lo];
			termDocs[lo] = termDocs[hi];
			termDocs[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (compareTermDocs(termDocs[lo], termDocs[mid]) > 0) {
		InnerCombine *tmp = termDocs[lo];
		termDocs[lo] = termDocs[mid];
		termDocs[mid] = tmp;
	}
	if (compareTermDocs(termDocs[mid], termDocs[hi]) > 0) {
		InnerCombine *tmp = termDocs[mid];
		termDocs[mid] = termDocs[hi];
		termDocs[hi] = tmp;
		if (compareTermDocs(termDocs[lo], termDocs[mid]) > 0) {
			InnerCombine *tmp2 = termDocs[lo];
			termDocs[lo] = termDocs[mid];
			termDocs[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	InnerCombine *partition = termDocs[mid];
	for (;;) {
		while (compareTermDocs(termDocs[right], partition) > 0) --right;
		while (left < right && compareTermDocs(termDocs[left], partition) <= 0) ++left;
		if (left < right) {
			InnerCombine *tmp = termDocs[left];
			termDocs[left] = termDocs[right];
			termDocs[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	quickSort(termDocs, lo, left);
	quickSort(termDocs, left + 1, hi);
}

TagDocIterator::TagDocIterator(IndexHandler *_handlers, DocIterator *_it, char *_data, int _dataLen, MinHeap * _minHeap, FilterInfo * _filterInfo) 
: DocIterator(_handlers) {
	docid = -1;
	cscore = 0.0;
	it = _it;
	data = _data;
	dataLen = _dataLen;
	filterInfo = _filterInfo;
	minHeap = _minHeap;
	filters = (Filter **)malloc(sizeof(Filter*) * 3);
	bzero(filters, sizeof(Filter*) * 3); 
	docs = NULL;
	size = 0;
	threod = 0.0f;
	firstTime = true;
	queryweights = NULL;
	state = 0;
	init();
}

TagDocIterator::~TagDocIterator() {
	if (queryweights != NULL) {
		free(queryweights);
		queryweights = NULL;
	}
	if (docs != NULL) {
		int i = 0;
		for (i = 0; i < size; i++) {
			MutiTermDocs *doc = docs[i];
			if (doc != NULL) delete doc;
			docs[i] = NULL;
		}
		free(docs);
		docs = NULL;
	}

	if (filters != NULL) {
		for (int i = 0; i < 3; i++) {
			Filter * filter = filters[i];
			if (filter != NULL) {
				delete filter;
				filters[i] = NULL;
			}
		}
		free(filters);
		filters = NULL;
	}
}

int TagDocIterator::docId() {
	return docid;
}

bool TagDocIterator::next() {                                       /* use WAND to get candidate doc */
	if (state == -1) return false;
	if (firstTime) {
		iload();
		firstTime = false;
	}
	if (!donext()) {
		docid = -1;
		state = -1;
		return false;
	} else 	return true;
}

void TagDocIterator::iload() {
	int i = 0;
	float total = 0.0f;
	for (; i < size; i++) {
		if (docs[i]->next()) total += docs[i]->queryweight;
	}
	threod = 0.05 * total;                                         /* set threshold of WAND */
	quickSort(docs, 0, size - 1);
}

int TagDocIterator::compareTermDocs(MutiTermDocs *p1, MutiTermDocs *p2) {
	return p1->docid - p2->docid; 
}

/* sort termDocs in increasing order of docid */
void TagDocIterator::quickSort(MutiTermDocs **termDocs, int lo, int hi) {
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (compareTermDocs(termDocs[lo], termDocs[hi]) > 0) {
			MutiTermDocs *tmp = termDocs[lo];
			termDocs[lo] = termDocs[hi];
			termDocs[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (compareTermDocs(termDocs[lo], termDocs[mid]) > 0) {
		MutiTermDocs *tmp = termDocs[lo];
		termDocs[lo] = termDocs[mid];
		termDocs[mid] = tmp;
	}
	if (compareTermDocs(termDocs[mid], termDocs[hi]) > 0) {
		MutiTermDocs *tmp = termDocs[mid];
		termDocs[mid] = termDocs[hi];
		termDocs[hi] = tmp;
		if (compareTermDocs(termDocs[lo], termDocs[mid]) > 0) {
			MutiTermDocs *tmp2 = termDocs[lo];
			termDocs[lo] = termDocs[mid];
			termDocs[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	MutiTermDocs *partition = termDocs[mid];
	for (;;) {
		while (compareTermDocs(termDocs[right], partition) > 0) --right;
		while (left < right && compareTermDocs(termDocs[left], partition) <= 0) ++left;
		if (left < right) {
			MutiTermDocs *tmp = termDocs[left];
			termDocs[left] = termDocs[right];
			termDocs[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	quickSort(termDocs, lo, left);
	quickSort(termDocs, left + 1, hi);
}

bool TagDocIterator::donext() {                           /* implements WAND, set docid to candidate */
	while (true) {
		int n = 1;
		int len = size;
		int mid;
		if (len >= 4) mid= len/2 -1;
		else mid = len - 1;
		if (docs[0]->docid > docs[mid]->docid) n = mid;              /* set n or len, the region to search position for docs[0] */
		else len = mid + 1;

		for (; n < len; n++) {
			if (docs[0]->docid < docs[n]->docid) break; 
		}
		if (n > 1) {                                                 /* insert docs[0] to the right position */
			MutiTermDocs *temp = docs[0];
			memcpy(docs, docs + 1, sizeof(MutiTermDocs*) * (n - 1));
			docs[n - 1] = temp;
		}
		float total = 0.0f;
		int i = 0;
		int flag = 0;
		for (; i < size; i++) {                                      /* if sum of UB is over threod, we have an candidate */
			total += docs[i]->ub * docs[i]->queryweight;
			if (total >= threod) break;
		}
		if (i == size || docs[i]->docid == INT_MAX) {                /* no candidate or a doc list reaches end */
			return false;
		}
		int curdoc = docs[i]->docid;
		if (curdoc <= docid) {                                       /* curdoc is already considered */
			docs[0]->skipTo(docid + 1);
			continue;
		} else {
			if (docs[0]->docid == curdoc) {                          /* we have an candidate */
				if (it != NULL) {
					if (it->docid < curdoc) it->skipTo(curdoc);
					if (it->docid == INT_MAX) {
						return false;
					} else if (it->docid == curdoc) {
						docid = curdoc;
						float count = 0.0f;
						for (i = 0; i < size; i++) {
							if (docs[i]->docid == curdoc) {
								count += docs[i]->weight() * docs[i]->queryweight;
							} else break;
						}
						if (NULL != minHeap) {
							if (minHeap->isFull()) threod = minHeap->getMinScore();
						}
						cscore = count;
						docs[0]->skipTo(curdoc + 1);
						return true;
					} else {
						docs[0]->skipTo(it->docid);
						continue;
					}
				} else {
					docid = curdoc;
					float count = 0.0f;
					for (i = 0; i < size; i++) {
						if (docs[i]->docid == curdoc) {
							count += docs[i]->weight() * docs[i]->queryweight;       /* calculate score of curdoc */
						} else break;
					}
					if (NULL != minHeap) {
						if (minHeap->isFull()) threod = minHeap->getMinScore();      /* update threod */
					}
					cscore = count;
					docs[0]->skipTo(curdoc + 1);
					return true;
				}
			} else {
				docs[0]->skipTo(curdoc);
				continue;
			}
		}
	}
}

bool TagDocIterator::skipTo(int target) {
	if (state == -1) return false;
	int i = 0;
	for (; i < size; i++) {
		if (docs[i]->docid != INT_MAX) docs[i]->skipTo(target);
	}
	quickSort(docs, 0, size - 1);
	if (!donext()) {
		docid = -1;
		state = -1;
		return false;
	} else return true;
}

float TagDocIterator::score() {
	return cscore;
}

void TagDocIterator::init() {
	if (handlers == NULL || data == NULL 
	|| dataLen <= 0 || ((dataLen % 9) != 0)) {
		state = -1;
		return;
	}
	int termnum = dataLen / 9, i = 0;
	Term *terms = (Term*)malloc(sizeof(Term) * termnum);
	queryweights = (float*)malloc(sizeof(float) * termnum);
	docs = (MutiTermDocs**)malloc(sizeof(MutiTermDocs*) * termnum);
	bzero(terms, sizeof(Term) * termnum);
	bzero(queryweights, sizeof(float) * termnum);
	bzero(docs, sizeof(MutiTermDocs*) * termnum);
	char temp[dataLen];
	for (i = 0; i < termnum; i++) {                                                  /* read simiquery into terms */
		terms[i].field = *(data + 9 * i);
		terms[i].data = (unsigned char*)data + 9 * i + 1;
		terms[i].dataLen = 6;
		queryweights[i] = Utils::encode(data + 9 * i + 7);
	}
	int couter = 0, base = 0;
	TermInvert invert;
	bool res = true;
	TermReader *reader = NULL;
	char *cousor = NULL;
	char *delFile = NULL;
	Handler *full = handlers->fullHandler();
	Handler *incr = handlers->incrHandler();
	Realtime *realtime = handlers->realtimeHandler();
	if (NULL != filterInfo ) {
		if (NULL != full) filters[0] = new DiskFilter(full, filterInfo);
		else filters[0] = NULL;
		if (NULL != incr) filters[1] = new DiskFilter(incr, filterInfo);
		else filters[1] = NULL;
		if (NULL != realtime) filters[2] = new RealFilter(realtime, filterInfo);
		else filters[2] = NULL;
	} else {
		for (i = 0; i < 3; i++) {
			filters[i] = NULL;
		}
	}
	int maxdoc = 0; 
	if (realtime != NULL) maxdoc = realtime->getDocnum();
	for (i = 0; i < termnum; i++) {                                                  /* init docs[i] */
		couter = 0;
		base = 0;
		TermDocs** subdocs = (TermDocs**)malloc(sizeof(TermDocs*) * 3);
		bzero(subdocs, sizeof(TermDocs*) * 3);
		if (full != NULL) {
			reader = handlers->fullTermReader();
			res = reader->term(terms[i], &invert);
			if (res && ((int)invert.off < (int)full->IndexDatSize)) {
				cousor = (char*)full->IndexDat;
				delFile = (char*)full->DelDat;
				subdocs[couter++] = new DiskTermDocs(invert.ub, base, cousor + (int)invert.off, delFile, invert.docnum, filters[0]);
			}
			base += full->docnum;
		}
		if (incr != NULL) {
			reader = handlers->incrTermReader();
			res = reader->term(terms[i], &invert);
			if (res && ((int)invert.off < (int)incr->IndexDatSize)) {
				cousor = (char*)incr->IndexDat;
				delFile = (char*)incr->DelDat;
				subdocs[couter++] = new DiskTermDocs(invert.ub, base, cousor + (int)invert.off, delFile, invert.docnum, filters[1]);
			}
			base += incr->docnum;
		}
		if (realtime != NULL) {
			reader = handlers->realTermReader();
			res = reader->term(terms[i], &invert);
			if (res && (invert.invertStart != -1)) {
				RtByteSliceReader *rtreader = new RtByteSliceReader();
				rtreader->init(realtime->termPostPool, invert.invertStart, invert.invertUpto);
				subdocs[couter++] = new RealTermDocs(invert.ub, base, maxdoc, rtreader, realtime, filters[2]);
			}
		}
		if (couter > 0) {
			int cursize = size;
			size ++;
			docs[cursize] = new MutiTermDocs(subdocs, couter);                       /* store into docs */
			docs[cursize]->queryweight = queryweights[i];
		} else {
			free(subdocs);
		}
	}
	free(terms);
	if (size == 0) state = -1;
}
