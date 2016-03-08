#include "analyzer.h"

Analyzer::Analyzer(IndexField *_field) {
	field = _field;
}

Analyzer::~Analyzer() {
	if (terms != NULL) free(terms);
}

int Analyzer::init(char *_data, int _length) {
	data = _data;
	length = _length;
	return 1;
}

TermInfo* Analyzer::getTerms() {
	return terms;
}

int Analyzer::getTermNum() {
	return termNum;
}

TextAnalyzer::TextAnalyzer(IndexField *_field) : Analyzer(_field) {
	emptyPayload = (char*)malloc(2);                                               /* payload is 0 */
	bzero(emptyPayload, 2);
	terms = (TermInfo*)malloc(sizeof(TermInfo));
}

TextAnalyzer::~TextAnalyzer() {
	free(emptyPayload);
}

int TextAnalyzer::analyze() {                                                      /* simply load data into terms, payload is 0 */
	if (data != NULL && length > 0) {
		terms[0].field = (char)field->getNumber();
		terms[0].data = data;
		terms[0].dataLen = length;
		terms[0].payload = emptyPayload;
		terms[0].payloadLen = 2;
		termNum = 1;
	} else termNum = 0;
	return 1;
}

TagsAnalyzer::TagsAnalyzer(IndexField *_field) : Analyzer(_field) {
	terms = (TermInfo*)malloc(sizeof(TermInfo) * MAX_TAGS_NUMBER);
}

TagsAnalyzer::~TagsAnalyzer() {
}

int TagsAnalyzer::analyze() {
	if (data != NULL && length > 0 && (length % 8) == 0) {
		int tagNumber = length / 8;                                                 /* tags are put together, each takes 8 bytes, no comma between */
		int i = 0, j = 0, checked = 0;
		for (i = 0; i < tagNumber; i++) {
			bool bingou = false;
			for (j = 0; j < checked; j++) {                                         /* skip repeat tag */
				if (memcmp(terms[j].data, data + (i * 8), 6) == 0) {
					bingou = true;
					break;
				}
			}
			if (bingou) continue;
			terms[checked].data = data + (i * 8);
			terms[checked].dataLen = 6;
			terms[checked].payload = data + (i * 8) + 6;
			terms[checked].payloadLen = 2;
			terms[checked].field = (char)field->getNumber();
			checked++;
		}
		termNum = checked;
	} else {
		termNum = 0;
	}
	return 1;
}

DnfAnalyzer::DnfAnalyzer(IndexField *_field, char* _levelDbPath) : Analyzer(_field) {
	db = NULL;
	levelDbPath = _levelDbPath;
	//printf("leveldb path %s\n", levelDbPath);
	options.create_if_missing = true;
	options.block_cache = leveldb::NewLRUCache(1048576);
	leveldb::Status status = leveldb::DB::Open(options, levelDbPath, &db);
	sizeKey = new leveldb::Slice(KEY_SIZE, strlen(KEY_SIZE));
	terms = (TermInfo*)malloc(sizeof(TermInfo) * MAX_DNF_NUMBER);
	newConjuction = (int*)malloc(sizeof(int) * MAX_CONJ_NUMBER);
	newConjuctionNum = 0;
	oldConjuction = (int*)malloc(sizeof(int) * MAX_CONJ_NUMBER);
	oldConjuctionNum = 0;
	termConjuctionId = (int*)malloc(sizeof(int) * MAX_DNF_NUMBER);
	termDataBuffer = (char*)malloc(sizeof(char) * 9 * MAX_DNF_NUMBER);
	flag = true;
	if (status.ok()) {
	    string value;
	    leveldb::Status s = db->Get(leveldb::ReadOptions(), *sizeKey, &value);
	    int size = 0;
	    if (s.ok() && value.c_str() != NULL) size = atoi(value.c_str());
	    else if (s.IsNotFound()) {                                             /* write original ConjId:1, 0 is for empty dnf */
		    stringstream newstr;
		    newstr << 1;
		    value = newstr.str();
		    s = db->Put(leveldb::WriteOptions(), *sizeKey, value);
		    if (!s.ok()) flag = false;
	    } else flag = false;
	} else flag = false;
	maxconjsize = -1;
}

DnfAnalyzer::~DnfAnalyzer() {
	free(termDataBuffer);
	free(newConjuction);
	free(oldConjuction);
	free(termConjuctionId);
	delete sizeKey;
	if (db != NULL) delete db;
}

int DnfAnalyzer::ready() {
	return flag ? 1 : 0;
}

int DnfAnalyzer::preAnalyze() {                                  /* check if dnf is in correct pattern */
	if (data != NULL && length > 0) {
		int i = 0, begin = 0, end = 0;
		int skip = 1;
		for (i = 0; i < length; i += skip) {
			char chr = data[i];
			bool flag = false;
			if (chr == DNF_OR) {
				if (data[begin] != GROUP_LEFT || i == 0 
				|| data[i - 1] != GROUP_RIGHT || i - begin < 13) {
					return 0;
				}
				end = i - 1;
				flag = true;
			}
			if (i == length - 1) {
				if (data[begin] != GROUP_LEFT || data[i] != GROUP_RIGHT 
				|| i - begin < 12) {
					return 0;
				}
				end = i;
				flag = true;
			}
			if (flag) {
				begin++; 
				end = end - 2;
			    if (!isConjuction(data + begin, end - begin + 1)) return 0;
				else {
					if (i == length - 1) return 1;
				}
				begin = i + 1;
			}
			if (chr == GROUP_LEFT) skip = 3;
			else if (chr == ATTRIBUTE_LEFT) skip = 7;
			else if (chr == ATTRIBUTE_SPLIT) skip = 7;
			else if (chr == DNF_AND) skip = 3;
			else skip = 1;
		}
	} else {
		return 1;
	}
	return 0;
}

int DnfAnalyzer::analyze() {
	if (preAnalyze() <= 0) return -1;
	termNum = 0;
	cursor = 0;
	newConjuctionNum = 0;
	oldConjuctionNum = 0;
	if (data != NULL && length > 0) {
		int i = 0, begin = 0, end = 0;
		int skip = 1;
		for (i = 0; i < length; i += skip) {
			char chr = data[i];
			bool flag = false;
			if (chr == DNF_OR) {
				if (data[begin] != GROUP_LEFT || i == 0 
				|| data[i - 1] != GROUP_RIGHT || i - begin < 13) {
					return 0;
				}
				end = i - 1;
				flag = true;
			}
			if (i == length - 1) {
				if (data[begin] != GROUP_LEFT || data[i] != GROUP_RIGHT 
				|| i - begin < 12) {
					return 0;
				}
				end = i;
				flag = true;
			}
			if (flag) {
				begin++; 
				end = end - 2;
			    if (!isConjuction(data + begin, end - begin + 1)) return 0;
			    leveldb::Slice *key = new leveldb::Slice(data + begin, end - begin + 1);
				string value;
			    leveldb::Status s = db->Get(leveldb::ReadOptions(), *key, &value);
			    if (s.ok() && value.c_str() != NULL) {                               /* conjunction already met, no need to analyze, only need to add docId in inverted data of conj in oldConjuction */
				    int conjId = atoi(value.c_str());
				    oldConjuction[oldConjuctionNum++] = conjId;
					delete key;
			    } else if (s.IsNotFound()) {                                         /* new conjunction */
				    s = db->Get(leveldb::ReadOptions(), *sizeKey, &value);           /* get new conjId */
				    int size = -1;
				    if (s.ok() && value.c_str() != NULL) size = atoi(value.c_str());
				    else return 0;
				    newConjuction[newConjuctionNum++] = size;
				    if (analyzeConjuction(data + begin, end - begin + 1, size) <= 0) return 0;
					stringstream sizestr;
					sizestr << size;
					value = sizestr.str();
					s = db->Put(leveldb::WriteOptions(), *key, value);               /* store conjunction and conjId */
					delete key;
				    if (!s.ok()) return 0;
				    size++;                                                          /* increase conjId for next new conjunction*/
				    stringstream newstr;
				    newstr << size;
				    value = newstr.str();
				    s = db->Put(leveldb::WriteOptions(), *sizeKey, value);
				    if (!s.ok()) return 0;
			    } else {
					delete key;
				    return 0;
			    }
			    begin = i + 1;
			}
			if (chr == GROUP_LEFT) skip = 3;
			else if (chr == ATTRIBUTE_LEFT) skip = 7;
			else if (chr == ATTRIBUTE_SPLIT) skip = 7;
			else if (chr == DNF_AND) skip = 3;
			else skip = 1;
		}
	} else {
		oldConjuction[oldConjuctionNum++] = 0;
	}
	return 1;
}

int DnfAnalyzer::analyzeConjuction(char* buf, int length, int conjuctionId) {   /* extract all term into terms */
	int i = 0, begin = 0, end = 0, conjsize = 0;
	int beginTermId = termNum, termId = 0;
	int skip = 1;
	for (i = 3; i <= length; i += skip) {
		char chr = buf[i];
		bool flag = false;
		if (chr == DNF_AND) {
			end = i - 1;
			flag = true;
		}
		if (i == length) {
			end = i;
			flag = true;
		}
		if (flag) {
		    bool belong = (buf[begin + 2] == BELONG);
		    if (belong) conjsize++;
		    int size = end - begin + 1;
		    while (size >= 11) {                                       /* split multiple assignment,ie: age<{3;4} */
			    termId = termNum++;
			    terms[termId].data = termDataBuffer + cursor;          /* key(2byte) value(6byte) */
			    terms[termId].dataLen = 8;
			    terms[termId].payload = termDataBuffer + cursor + 8;   /* belong(1byte) */
			    terms[termId].payloadLen = 1;
			    termConjuctionId[termId] = conjuctionId;
			    memcpy(termDataBuffer + cursor, buf +begin, sizeof(char) * 2);
			    cursor += 2;
			    memcpy(termDataBuffer + cursor, buf + begin + size - 7, sizeof(char) * 6);
			    cursor += 6;
			    if (belong) termDataBuffer[cursor++] = 1;
			    else termDataBuffer[cursor++] = 0;
				size -= 7;
		    }
		    begin = i + 1;
		}
		if (chr == ATTRIBUTE_LEFT) skip = 7;
		else if (chr == ATTRIBUTE_SPLIT) skip = 7;
		else if (chr == DNF_AND) skip = 3;
		else skip = 1;
	}
	if (conjsize == 0) {                                                /* add a Z term */
		termId = termNum++;
		terms[termId].data = termDataBuffer + cursor;
		terms[termId].dataLen = 8;
		terms[termId].payload = termDataBuffer + cursor + 8;
		terms[termId].payloadLen = 1;
		bzero(termDataBuffer + cursor, sizeof(char) * 8);
		cursor += 8;
		termDataBuffer[cursor++] = 1;                                   /* belong */
		termConjuctionId[termId] = conjuctionId;
	}
	if (conjsize > maxconjsize) {
		maxconjsize = conjsize;
	}
	int endTermId = termNum;
	for (; beginTermId < endTermId; beginTermId++) {                    /* conjunction size */
		terms[beginTermId].field = (char)conjsize;
	}
	return 1;
}

bool DnfAnalyzer::isConjuction(char* buf, int length) {
	if (length < 10) return false;
	int i = 0, begin = 0, end = 0;
	int skip = 1;
	for (i = 3; i <= length; i += skip) {
		char chr = buf[i];
		bool flag = false;
		if (chr == DNF_AND) {
			end = i - 1;
			flag = true;
		}
		if (i == length) {
			end = i;
			flag = true;
		}
		if (flag) {
		    bool res = true;
		    int size = end - begin + 1;
		    res = res && size >= 11 && (size - 4) % 7 == 0;
		    res = res && (buf[begin + 2] == BELONG || buf[begin + 2] == NOT_BELONG) 
		    && buf[begin + 3] == ATTRIBUTE_LEFT && buf[end] == ATTRIBUTE_RIGHT;
		    if (!res) return false;
		    size = size - 1;
		    while (size >= 17) {
			    size -= 7;
			    if (buf[begin + size] != ATTRIBUTE_SPLIT) return false;
		    }
		    begin = i + 1;
		}
		if (chr == ATTRIBUTE_LEFT) skip = 7;
		else if (chr == ATTRIBUTE_SPLIT) skip = 7;
		else if (chr == DNF_AND) skip = 3;
		else skip = 1;
	}
	return true;
}

int DnfAnalyzer::getNewConjuctionNum() {
	return newConjuctionNum;
}

int DnfAnalyzer::getOldConjuctionNum() {
	return oldConjuctionNum;
}

int* DnfAnalyzer::getNewConjuction() {
	return newConjuction;
}

int* DnfAnalyzer::getOldConjuction() {
	return oldConjuction;
}

int* DnfAnalyzer::termConjuction() {
	return termConjuctionId;
}

int DnfAnalyzer::getMaxConjSize() {
	return maxconjsize;
}
