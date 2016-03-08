#include "term.h"

TermReader::TermReader() {
}

TermReader::~TermReader() {
}

DiskTermReader::DiskTermReader(Handler *_handler) {
	handler = _handler;
	if (handler != NULL) {
		termidx = (char*)handler->TermIdx;
		termdat = (char*)handler->TermDat;
		dnftermidx = (char*)handler->DnfTermIdx;
		dnftermdat = (char*)handler->DnfTermDat;
		termidxlen = (int)handler->TermIdxSize;
		termdatlen = (int)handler->TermDatSize;
		dnftermidxlen = (int)handler->DnfTermIdxSize;
		dnftermdatlen = (int)handler->DnfTermDatSize;
		termindexs = NULL;
		termbuffer = NULL;
		termindexlen = 0;
		dnftermindexlen = 0;
		cousor = NULL;
		flag = false;
		if (termidx != NULL || termdat != NULL || dnftermidx != NULL 
		|| dnftermdat != NULL || termidxlen >= 8 || dnftermidxlen >= 8) {
		    init();
		}
	} else flag = false;
}

DiskTermReader::~DiskTermReader() {
	if (termbuffer != NULL) {
		free(termbuffer);
		termbuffer = NULL;
	}
	if (termindexs != NULL) {
		free(termindexs);
		termindexs = NULL;
	}
}

bool DiskTermReader::term(Term term, TermInvert *invert) {              /* read inverted-data of term into invert */
	if (!flag) return false;
	int id = getIndex(term), couter = 0;                                /* find low end of region of term in term.idx */
	if (id < 0) return false;
	TermIndex index = termindexs[id];
	char *start = termdat + (int)index.termoff + 8;                     /* 8: version + termcount */
	while ((couter < 128) && ((start - termdat) < termdatlen)) {
		int shift = 7, b = (*(start++)) & 0xff;                         /* read term length */
		int i = b & 0x7F;
		long j = 0;
		for (; (b & 0x80) != 0; shift += 7) {
			b = (*(start++)) & 0xff;
			i |= (b & 0x7F) << shift;
		}
		int res = compareTerm(term, start, i);
		if (res < 0) return false;
		else if (res == 0) {                                            /* find term, read inverted-data */
			start += i;
			b = (*(start++)) & 0xff;
			i = b & 0x7F;
			for (shift = 7; (b & 0x80) != 0; shift += 7) {
				b = (*(start++)) & 0xff;
				i |= (b & 0x7F) << shift;
			}
			invert->docnum = i;
			b = (*(start++)) & 0xff;
			j = b & 0x7F;
			for (shift = 7; (b & 0x80) != 0; shift += 7) {
				b = (*(start++)) & 0xff;
				j |= (b & 0x7FL) << shift;
			}
			invert->off = j;
			invert->ub = Utils::encode(start);
			return true;
		} else {
			start += i;
			while (((*start) & 0x80) != 0) start++;
		    start++;
			while (((*start) & 0x80) != 0) start++;
			start += 3;
			couter++;
		}
	}
	return false;
}

bool DiskTermReader::dnfterm(Term term, DnfTermInvert *invert) {
	if (!flag) return false;
	int termoff = getDnfIndex(term), counter = 0;
	if (termoff < 0) return false;
	if (termoff == dnftermindexlen - 1) termoff--;
	termoff = 8 + termoff * 128 * 21;
	char *start = dnftermdat + termoff;
	while ((counter < 128) && ((start - dnftermdat) < dnftermdatlen)) {
		int res = compareTerm(term, start, 9);
		if (res < 0) return false;
		else if (res == 0) {
			start += 9;
			invert->conjnum = *((int*)start);
			start += 4;
			invert->off = *((long*)start);
			return true;
		} else {
			start += 21;
			counter++;
		}
	}
	return false;
}

int DiskTermReader::getDnfIndex(Term term) {
    int lo = 0;
    int hi = dnftermindexlen - 1;
    while (hi >= lo) {
		int mid = (lo + hi) >> 1;
		int delta = compareTerm(term, dnftermidx + 8 + (21 * mid), 9);
		if (delta < 0) hi = mid - 1;
		else if (delta > 0) lo = mid + 1;
		else return mid;
    }
    return hi;
}

int DiskTermReader::compareTerm(Term term, char* data, int datalen) {              /* compare term: fieldId+fieldData */
	if ((int)(term.field) == (int)(data[0])) {                                     /* same fieldId, compare data */
		char *text1 = (char*)term.data;
		char *text2 = data + 1;
		if (term.dataLen > (datalen - 1)) {
			int res = memcmp(text2, text1, datalen - 1);
			if (res > 0) return -1;
			else if (res == 0) return 1;
			else return 1;
		} else if (term.dataLen == (datalen - 1)) {
			return memcmp(text1, text2, term.dataLen);
		} else {
			int res = memcmp(text1, text2, term.dataLen);
			if (res > 0) return 1;
			else if (res == 0) return -1;
			else return -1; 
		}
	} else return (int)(term.field) - (int)(data[0]);                              /* compare fieldId */
}

int DiskTermReader::getIndex(Term term) {                                          /* find low end of region of term in term.idx */
    int lo = 0;
    int hi = termindexlen - 1;
    while (hi >= lo) {
		int mid = (lo + hi) >> 1;
		int delta = compareTerm(term, termindexs[mid].data, termindexs[mid].dataLen);
		if (delta < 0) hi = mid - 1;
		else if (delta > 0) lo = mid + 1;
		else return mid;
    }
    return hi;
}

void DiskTermReader::init() {                                                       /* load term.idx into termindexs */
	cousor = dnftermidx + 4;
	dnftermindexlen = *((int*)cousor);
    cousor = termidx + 4;
	int termnum = *((int*)cousor), pos = 0;
	cousor += 4;
	if (dnftermindexlen > 0 && termnum > 0) {
		termindexs = (TermIndex*)malloc(sizeof(TermIndex) * termnum);
		termbuffer = (char*)malloc(sizeof(char) * termnum * 20);
		bzero(termbuffer, sizeof(char) * termnum * 20);
		for (int i = 0; i < termnum; i++) {
			termindexs[i].dataLen = readVInt();
			termindexs[i].data = termbuffer + pos;
			memcpy(termbuffer + pos, cousor, sizeof(char) * termindexs[i].dataLen);
			pos += termindexs[i].dataLen;
			cousor += termindexs[i].dataLen;
			termindexs[i].docnum = readVInt();
			termindexs[i].indexoff = readVLong();
			termindexs[i].ub = readUb();
			termindexs[i].termoff = readVLong();
		}
		termindexlen = termnum;
		flag = true;
	}
}

int DiskTermReader::readVInt() {
	int shift = 7, b = (*(cousor++)) & 0xff;
    int i = b & 0x7F;
    for (; (b & 0x80) != 0; shift += 7) {
		b = (*(cousor++)) & 0xff;
		i |= (b & 0x7F) << shift;
    }
    return i;
}

long DiskTermReader::readVLong() {
	int shift = 7, b = (*(cousor++)) & 0xff;
    long i = b & 0x7F;
    for (; (b & 0x80) != 0; shift += 7) {
		b = (*(cousor++)) & 0xff;
		i |= (b & 0x7FL) << shift;
    }
    return i;
}

float DiskTermReader::readUb() {
	char temp[2];
	temp[0] = *(cousor++);
	temp[1] = *(cousor++);
	return Utils::encode(temp);
}

RealTermReader::RealTermReader(Realtime *_realtime) {
	realtime = _realtime;
}

RealTermReader::~RealTermReader() {
}

bool RealTermReader::term(Term term, TermInvert *invert) {                 /* read inverted-data of term into invert */
	TermDict *dict = realtime->termDict;
	int id = dict->getTerm(term);
	if (id == -1) return false;
	RawPostingList *list = dict->getPostList(id);
	if (list == NULL || (list->invertedStart == -1)) return false;
	invert->ub = list->ub;
	invert->invertStart = list->invertedStart;
	invert->invertUpto = list->invertedUpto;
	return true;
}

bool RealTermReader::dnfterm(Term term, DnfTermInvert *invert) {
	TermDict *dict = realtime->termDict;
	int id = dict->getDnfTerm(term);
	if (id == -1) return false;
	DnfRawPostingList *list = dict->getDnfPostList(id);
	if (list == NULL || (list->invertedStart == -1)) return false;
	invert->invertStart = list->invertedStart;
	invert->invertUpto = list->invertedUpto;
	return true;
}
