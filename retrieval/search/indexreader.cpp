#include "indexreader.h"

ConjDocs::ConjDocs(int _base) {
	base = _base;
	docid = -1;
}

ConjDocs::~ConjDocs() {
}

DiskConjDocs::DiskConjDocs(int _base, char* _data, char* _deldata, int _docnum, Filter *_filter) 
: ConjDocs(_base) {
	data = _data;
	deldata = _deldata;
	docnum = _docnum;
	filter = _filter;
	if (_base < 0 || data == NULL || docnum <= 0) {
		flag = false;
		docid = INT_MAX;
	} else flag = true;
}

DiskConjDocs::~DiskConjDocs() {
}

/*
bool DiskConjDocs::isDelete(int id) {
	if (NULL == deldata) return false;     
	int off = id/8;                                        
	char a = *(char *)(deldata + off);                     
	if ( (a & (1<<(7-id%8)))== (1<<(7-id%8)) ) return true;
	else return false;  
}*/

int DiskConjDocs::docId() {
	return docid;
}

bool DiskConjDocs::next() {
	if (!flag) return false;
	if (docnum > 0) {
		int rid = *((int*)data);
		docnum--;
		data += DOC_ID_BYTE_NUM;
	/*	bool ret;
		while(docnum > 0) {
			ret = ISDELETE(rid, deldata);
			if (!ret) {
				if (NULL != filter) {
					ret = filter->filterData(rid);
					if (ret) break;
				} else break;
			} 
			rid = *((int*)data);
			data += DOC_ID_BYTE_NUM;
			docnum--;
		}
		if (docnum <= 0) {
			docid = INT_MAX;
			return false;
		}
*/
		
		if (NULL != filter) {
			bool ret = filter->filterData(rid);
			while (!ret && docnum > 0) {
				rid = *((int*)data);
				data += DOC_ID_BYTE_NUM;
				docnum--;
				ret = filter->filterData(rid);
			}
			if (!ret && docnum <= 0) {
				docid = INT_MAX;
				return false;
			}
		} else {
			while(ISDELETE(rid, deldata)) {
				if (docnum > 0) {
					rid = *((int*)data);
					data += DOC_ID_BYTE_NUM;
					docnum--;
				} else {
					docid = INT_MAX;
					return false;
				}
			}
		}
		docid = rid + base;
		return true;
	} else {
		docid = INT_MAX;
		return false;
	}
}

RealConjDocs::RealConjDocs(int _base, int _maxdoc, RtByteSliceReader * _reader, Realtime *_rtime, Filter *_filter) 
: ConjDocs(_base) {
	maxdoc = _maxdoc;
	reader = _reader;
	rtime = _rtime;
	filter = _filter;
	if (_base < 0 || maxdoc <= 0 || reader == NULL || rtime == NULL) {
		flag = false;
		docid = INT_MAX;
	} else flag = true;
}

RealConjDocs::~RealConjDocs() {
	if (NULL != reader) {
		delete reader;
		reader = NULL;
	}
}

int RealConjDocs::docId() {
	return docid;
}

bool RealConjDocs::next() {
	if (!flag) return false;
	int rid = readVint(reader);
	if (rid == -1 || rid >= maxdoc) {
		docid = INT_MAX;
		return false;
	}
/*
	bool ret;
	while(rid != -1 && rid < maxdoc) {
		ret = rtime->isDeleted(rid);
		if (!ret) {
			if (NULL != filter) {
				ret = filter->filterData(rid);
				if (ret) break;
			} else break;
		}
		rid = readVint(reader);
	}
	*/
	
	if (NULL != filter) {
		while (rid != -1 && rid < maxdoc && !filter->filterData(rid)) {
			rid = readVint(reader);
		}
		if (rid == -1 || rid >= maxdoc) { 
			docid = INT_MAX;
			return false;
		}	
	} else { 
		while(rid != -1 && rid < maxdoc && rtime->isDeleted(rid)) {
			rid = readVint(reader);
		}
		if (rid == -1 || rid >= maxdoc) {
			docid = INT_MAX;
			return false;
		}
	}
	docid = rid + base;
	return true;
}

Combine::Combine(ConjDocs **_docs, int _size) {
	docs = _docs;
	size = _size;
	docid = -1;
	begin = 0;
	end = 0;
	fillFlag = false;
    markFlag = 0;
	conjListEndNum = 0;
	bzero(mark, BITMAP_BLOCK_SIZE);
	conjListEnd = (int *)malloc(sizeof(int) * size);
	bzero(conjListEnd, sizeof(int) * size);
	if (!init()) isInit = false;
	else isInit = true;
}

Combine::~Combine() {
	if (NULL != conjListEnd) {
		free(conjListEnd);
		conjListEnd = NULL;
	}
}

bool Combine::init() {
	if (NULL == docs) return false;
	for (int i = 0; i < size; i++) {
		if (NULL == docs[i]) return false;
		bool ret = docs[i]->next();
		if (!ret) {
			conjListEnd[i] = 1;
			conjListEndNum++;
		}
	}
	return true;
}

int Combine::docId() {
	return docid;
}

bool Combine::next() {
	if (!isInit) return false;
	if (!fillFlag) {
		bool ret = fill();
		if (!ret) {
			docid = INT_MAX;
			return false;
		}
	}
	while (markFlag < BITMAP_BLOCK_SIZE) {
		if (mark[markFlag++] == 1) {
			docid = begin - BITMAP_BLOCK_SIZE + markFlag - 1;
			return true;
		}
	}
	if (conjListEndNum >= size) { 
		docid = INT_MAX;
		return false;
	}
	fillFlag = false;
	return next();
}

int Combine::skipTo(int target) {
	bool ret;
	if (!isInit) {
		docid = INT_MAX;
		return INT_MAX;
	}
	if (docid >= target) return docid;
	if ((target >= begin && begin != 0) || (target >= (begin + BITMAP_BLOCK_SIZE) && begin == 0)) {
		begin = (target/BITMAP_BLOCK_SIZE) * BITMAP_BLOCK_SIZE;
		fillFlag = false;
		bzero(mark, BITMAP_BLOCK_SIZE);
	}
	if (!fillFlag) {
		ret = fill();
		if (!ret) {
			docid = INT_MAX;
			return INT_MAX;
		}
	}
	if (begin - BITMAP_BLOCK_SIZE < target) markFlag = target % BITMAP_BLOCK_SIZE;
	while (markFlag < BITMAP_BLOCK_SIZE) {
		if (mark[markFlag++] == 1) {
			docid = begin - BITMAP_BLOCK_SIZE + markFlag - 1;
			return docid;
		}
	}
	if (conjListEndNum >= size) {
		docid = INT_MAX;
		return INT_MAX;
	}
	fillFlag = false;
	return skipTo(target);
}

bool Combine::fill() {
	int i = 0, id = 0;
	bool ret = false;
	end = begin + BITMAP_BLOCK_SIZE;
	bzero(mark, BITMAP_BLOCK_SIZE);
	if (conjListEndNum >= size) return false;
	for (i = 0; i< size; i++) {
		if (conjListEnd[i] == 1) continue;
		id = docs[i]->docid;
		if (id >= end) continue;
		if (id >= begin) mark[id - begin] = 1;
		while (ret = docs[i]->next()) {
			id = docs[i]->docid;
			if (id < end) {
				if (id >= begin) {
					mark[id - begin] = 1;
				}
			} else break;
		}
		if (!ret) {
			conjListEndNum ++;
			conjListEnd[i] = 1;
		}
	}
	begin += BITMAP_BLOCK_SIZE;
	fillFlag = true;
	markFlag = 0;
	return true;
}

DnfTermDocs::DnfTermDocs() {
	conjid = -1;
	isbelong = false;
}

DnfTermDocs::~DnfTermDocs() {
}

DiskDnfTermDocs::DiskDnfTermDocs(char *_data, int _conjnum) {
	data = _data;
	conjnum = _conjnum;
	if (data == NULL || conjnum <= 0) {
		flag = false;
		conjid = INT_MAX;
	} else flag = true;
}

DiskDnfTermDocs::~DiskDnfTermDocs() {
}

int DiskDnfTermDocs::conjId() {
	return conjid;
}

bool DiskDnfTermDocs::belong() {
	return isbelong;
}

bool DiskDnfTermDocs::next() {                      /* read conjid and isbelong */
	if (!flag) return false;
	if (conjnum > 0) {
		int rid = *((int*)data);
		data += CONJ_ID_BYTE_NUM;
		int temp = (int)(*data); 
		data += CONJ_BELONG_BYTE_NUM;
		if (rid < 0) {
			conjid = INT_MAX;
			return false;
		}
		conjid = rid;
		isbelong = (temp == 1) ? true : false;
		conjnum--;
		return true;
	} else {
		conjid = INT_MAX;
		return false;
	}
}

int DiskDnfTermDocs::skipTo(int target) {
	if (!flag) return INT_MAX;
	if (conjid >= target) return conjid;
	while (conjnum > 0) {
		conjid = *((int*)data);
		if (conjid < target) {
			data += CONJ_ID_BYTE_NUM + CONJ_BELONG_BYTE_NUM;
			conjnum--;
			continue;
		}
		data += CONJ_ID_BYTE_NUM;
		int temp = (int)(*data);
		data += CONJ_BELONG_BYTE_NUM;
		isbelong = (temp == 1) ? true : false;
		conjnum--;
		return conjid;
	}
	conjid = INT_MAX;
	return INT_MAX;
}

RealDnfTermDocs::RealDnfTermDocs(int _maxconj, RtByteSliceReader *_reader) {
	maxconj = _maxconj;
	reader = _reader;
	if (maxconj <= 0 || reader == NULL) {
		flag = false;
		conjid = INT_MAX;
	} else flag = true;
}

RealDnfTermDocs::~RealDnfTermDocs() {
	if (NULL != reader) {
		delete reader;
		reader = NULL;
	}
}

int RealDnfTermDocs::conjId() {
	return conjid;
}

bool RealDnfTermDocs::belong() {
	return isbelong;
}

bool RealDnfTermDocs::next() {
	if (!flag) return false;
	int rid = readVint(reader);
	if (rid == -1 || rid >= maxconj) {
		conjid = INT_MAX;
		return false;
	}
	if (reader->eof()) {
		conjid = INT_MAX;
		return false;
	}
	int temp = (int)(reader->readByte());
	conjid = rid;
	isbelong = (temp == 1) ? true : false;
	return true;
}

int RealDnfTermDocs::skipTo(int target) {
	if (!flag) return INT_MAX;
	if (conjid >= target) return conjid;
	bool ret = next();
	while (ret) {
		if (conjid >= target) return conjid;
		ret = next();
	}
	conjid = INT_MAX;
	return INT_MAX;
}

TermDocs::TermDocs(float _ub, int _base) {
	ub = _ub;
	base = _base;
	docid = -1;
	bzero(wei, 3);
}

TermDocs::TermDocs() {
	ub = 0;
	base = 0;
	docid = -1;
	bzero(wei, 3);
}

TermDocs::~TermDocs() {
}

float TermDocs::getub() {
	return ub;
}

int TermDocs::getbase() {
	return base;
}

MutiTermDocs::MutiTermDocs(TermDocs **_termdocs, int _size) {
	termdocs = _termdocs;
	size = _size;
	curoff = 0;
	curtermdoc = NULL;
	flag = false;
	queryweight = 0.0;
	if (termdocs != NULL && size > 0) {
		float max = 0.0f;
		for (int i = 0; i < size; i++) {
			TermDocs *termdoc = termdocs[i];
			if (termdoc != NULL) {
				flag = true;
				max = (max > termdoc->ub) ? max : termdoc->ub;
				if (curtermdoc == NULL) {
					curtermdoc = termdoc;
					curoff = i;
				}
			}
		}
		ub = max;
	}
	if (!flag) docid = INT_MAX;
}

MutiTermDocs::~MutiTermDocs() {
	if (NULL != termdocs) {
		for (int i = 0; i < size; i++) {
			if (NULL != termdocs[i]) {
				delete termdocs[i];
				termdocs[i] = NULL;
			}
		}
		free(termdocs);
		termdocs = NULL;
	}
}

int MutiTermDocs::docId() {
	return docid;
}

float MutiTermDocs::weight() {
	if (NULL == curtermdoc) return -1.0;
	else return curtermdoc->weight();
}

/* read doc list in full, incr, realtime in order, true if there is doc which passes filter and isn't deleted in doc list */
bool MutiTermDocs::next() {
	if (!flag) return false;
	if (curtermdoc == NULL) return false;
	if (curtermdoc->next()) {                                    /* curtermdoc available */
		docid = curtermdoc->docid;
		return true;
	} else {
		for (++curoff; curoff < size; curoff++) {
			if (termdocs[curoff] != NULL) break;
		}
		if (curoff < size) {
			curtermdoc = termdocs[curoff];                       /* move to next termdoc */
			return next();
		} else {
			curtermdoc = NULL;
			docid = INT_MAX;
			return false;
		}
	}
}

/* skip doc list to target */
int MutiTermDocs::skipTo(int target) {
	if (!flag) return INT_MAX;
	if (curtermdoc == NULL) {
		docid = INT_MAX;
		return INT_MAX;
	}
	int sid = curtermdoc->skipTo(target);
	if (sid != INT_MAX) {
		docid = sid;
		return docid;
	} else {
		for (++curoff; curoff < size; curoff++) {
			if (termdocs[curoff] != NULL) break;
		}
		if (curoff < size) {
			curtermdoc = termdocs[curoff];
			return skipTo(target);
		} else {
			curtermdoc = NULL;
			docid = INT_MAX;
			return INT_MAX;
		}
	}
}

/* _data: inverted data */
DiskTermDocs::DiskTermDocs(float _ub, int _base, char *_data, char* _deldata, int _docnum, Filter *_filter)
:TermDocs(_ub, _base) {
	data = _data;
	deldata = _deldata;
	docnum = _docnum;
	filter = _filter;
	if (_base < 0 || data == NULL || docnum <= 0) {
		flag = false;
		docid = INT_MAX;
	} else flag = true;
}

DiskTermDocs::~DiskTermDocs() {
}

/*bool DiskTermDocs::isDelete(int id) {
	if (NULL == deldata) return false;     
	int off = id/8;                                        
	char a = *(char *)(deldata + off);                     
	if ( (a & (1<<(7-id%8)))== (1<<(7-id%8)) ) return true;
	else return false;
}*/

int DiskTermDocs::docId() {
	return docid;
}

float DiskTermDocs::weight() {
	return Utils::encode((char *)wei);
}

bool DiskTermDocs::next() {                                             /* true if there is doc which passes filter and isn't deleted in doc list */
	if (!flag) return false;
	if (docnum > 0) {
		int rid = *(int*)data;                                          /* docId */
		data += DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM;                  /* jump to next doc in list */
		docnum--;
		/*
		bool ret;
		while(docnum > 0) {
			ret = ISDELETE(rid, deldata);
			if (!ret) {
				if (NULL != filter) {
					ret = filter->filterData(rid);
					if (ret) break;
				} else break;
			}
			rid = *((int*)data);
			data += DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM;
			docnum--;
		}
		if (docnum <= 0) {
			docid = INT_MAX;
			return false;
		}*/
		
		if (NULL != filter){
			bool ret = filter->filterData(rid);                          /* filter */
			while (!ret && docnum > 0) {
				rid = *(int*)data;
				data += DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM;
				docnum--;
				ret = filter->filterData(rid);
			}
			if (!ret && docnum <= 0) {
				docid = INT_MAX;
				bzero(wei, 3);
				return false;
			}
		} else {
			while (ISDELETE(rid, deldata)) {                             /* check if doc is deleted */
				if (docnum > 0) {
					rid = *(int*)data;
					data += DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM;
					docnum--;
				} else {
					docid = INT_MAX;
					bzero(wei, 3);
					return false;
				}
			}
		}
		wei[0] = (data-DOC_WEIGHT_BYTE_NUM)[0];
		wei[1] = (data-DOC_WEIGHT_BYTE_NUM)[1];
		docid = rid + base;
		return true;
	} else {
		docid = INT_MAX;
		bzero(wei, 3);
		return false;
	}
}

int DiskTermDocs::skipTo(int target) {
	if (docid >= target) return docid;
	if (!flag || docnum <= 0) {
		docid = INT_MAX;
		return INT_MAX;
	}
	if (target <= base) {
		next();
		return docid;
	}
	while (next()) {
		if (docid >= target) return docid;
	}

	docid = INT_MAX;
	return INT_MAX;
}

bool DiskTermDocs::jumpTable(int target) {
	while (docnum > 0) {
		int skipnum = JUMP_STEP(docnum) > (docnum - 1) ? (docnum - 1) : JUMP_STEP(docnum);
		char *ptr = data + skipnum * (DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM);
		int curid = *((int*)ptr);
		if (curid == target) {
			data = ptr;
			docnum -= skipnum;
			return true;
		} else if (curid > target) {
			int res = getJumpPos(0, skipnum, target);
			if (res == -1) return true;
			data += res * (DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM);
			if (*((int*)data) < target) {
				res++;
				data += (DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM);
			}
			docnum -= res;
			return true;
		} else {
			data = ptr + (DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM);
			docnum -= skipnum + 1;
		}
	}
	return false;
}

int DiskTermDocs::compareTo(int beg,int mid, int to) {
	char *dataPtr = data + (beg + mid) * (DOC_ID_BYTE_NUM + DOC_WEIGHT_BYTE_NUM);
	int curId;
	curId = *((int*)dataPtr);
	if (curId > to) return -1;
	else if (curId < to) return 1;
	else return 0;
}

int DiskTermDocs::getJumpPos(int beg, int end, int to) {
	int lo = 0;
	int hi = end - beg;
	while (hi >= lo) {
		int mid = (lo + hi) >> 1;
		int delta = compareTo(beg, mid, to);
		if (delta < 0) hi = mid - 1;
		else if (delta > 0) lo = mid + 1;
		else return mid;
	}
	return hi;
}

RealTermDocs::RealTermDocs(float _ub, int _base, int _maxdoc, RtByteSliceReader *_reader, Realtime *_rtime, Filter *_filter) 
: TermDocs(_ub, _base) {
	reader = _reader;
	rtime = _rtime;
	maxdoc = _maxdoc;
	filter = _filter;
	if (reader == NULL || rtime == NULL || maxdoc <= 0) {
		flag = false;
		docid = INT_MAX;
	} else flag = true;
}

RealTermDocs::~RealTermDocs() {
	if (NULL != reader) {
		delete reader;
		reader = NULL;
	}
}

int RealTermDocs::docId() {
	return docid;
}

float RealTermDocs::weight() {
	return Utils::encode((char *)wei);
}

bool RealTermDocs::next() {
	if (!flag) return false;
	int rid = readVint(reader);
	if (rid == -1 || rid >= maxdoc) {
		docid = INT_MAX;
		return false;
	}
	if (reader->eof()) {
		docid = INT_MAX;
		return false;
	}
	wei[0] = reader->readByte();
	if (reader->eof()) {
		docid = INT_MAX;
		return false;
	}
	wei[1] = reader->readByte();
/*
	bool ret;
	while (rid != -1 && rid < maxdoc) {
		ret = rtime->isDeleted(rid);
		if (!ret) {
			if (NULL != filter) {
				ret = filter->filterData(rid);
				if (ret) break;
			} else break;
		}

		rid = readVint(reader);
		if (reader->eof()) {
			docid = INT_MAX;
			bzero(wei, 3);
			return false;
		} else wei[0] = reader->readByte();
		if (reader->eof()) {
			docid = INT_MAX;
			bzero(wei, 3);
			return false;
		} else wei[1] = reader->readByte();
	}
	*/
	
	if (NULL != filter) {
		while (rid != -1 && rid < maxdoc && !filter->filterData(rid) ) {
			rid = readVint(reader);
			if (reader->eof()) {
				docid = INT_MAX;
				bzero(wei, 3);
				return false;
			} else wei[0] = reader->readByte();
			if (reader->eof()) {
				docid = INT_MAX;
				bzero(wei, 3);
				return false;
			} else wei[1] = reader->readByte();
		}
		if (rid == -1 || rid >= maxdoc) {
			docid = INT_MAX;
			bzero(wei, 3);
			return false;
		}
	} else {
		while (rid != -1 && rid < maxdoc && rtime->isDeleted(rid)) {
			rid = readVint(reader);
			if (reader->eof()) {
				docid = INT_MAX;
				bzero(wei, 3);
				return false;
			} else wei[0] = reader->readByte();
			if (reader->eof()) {
				docid = INT_MAX;
				bzero(wei, 3);
				return false;
			} else wei[1] = reader->readByte();
		}
		if (rid == -1 || rid >= maxdoc) {
			docid = INT_MAX;
			bzero(wei, 3);
			return false;
		}
	}
	docid = rid + base;
	return true;
}

int RealTermDocs::skipTo(int target) {
	if (!flag) return INT_MAX;
	if (docid >= target) return docid;
	while (next()) {
		if (docid >= target) return docid;
	}
	docid = INT_MAX;
	return INT_MAX;
}

int readVint(RtByteSliceReader *reader) {
	if (reader->eof()) return -1;
	int shift = 7, b = reader->readByte();
	int i = b & 0x7F;
	for (; (b & 0x80) != 0; shift += 7) {
		if (reader->eof()) return -1;
		b = reader->readByte() & 0xff;
		i |= (b & 0x7F) << shift;
	}
	return i;
}

float readWeight(RtByteSliceReader *reader) {
	char ub[2];
	if (reader->eof()) return -1.0;
	ub[0] = reader->readByte();
	if (reader->eof()) return -1.0;
	ub[1] = reader->readByte();
	return Utils::encode(ub);
}
