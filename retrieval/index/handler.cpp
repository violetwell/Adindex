#include "handler.h"

IndexHandler::IndexHandler(Handler *_full, Handler *_incr, Realtime *_realtime) {
	full = _full;
	incr = _incr;
	realtime = _realtime;
	if (full != NULL) fullReader = new DiskTermReader(full);
	else fullReader = NULL;
	if (incr != NULL) incrReader = new DiskTermReader(incr);
	else incrReader = NULL;
	if (realtime != NULL) realReader = new RealTermReader(realtime);
	else realReader = NULL;
}

IndexHandler::~IndexHandler() {
	if (fullReader != NULL) {
		delete fullReader;
		fullReader = NULL;
	}
	if (incrReader != NULL) {
		delete incrReader;
		incrReader = NULL;
	}
	if (realReader != NULL) {
		delete realReader;
		realReader = NULL;
	}
}

void IndexHandler::setFullHandler(Handler *_full) {
	full = _full;
	if (full != NULL) fullReader = new DiskTermReader(full);
}

void IndexHandler::setIncrHandler(Handler *_incr) {
	incr = _incr;
	if (incr != NULL) incrReader = new DiskTermReader(incr);
}

void IndexHandler::setRealtimeHandler(Realtime *_realtime) {
	realtime = _realtime;
	if (realtime != NULL) realReader = new RealTermReader(realtime);
}

Handler* IndexHandler::fullHandler() {
	return full;
}

Handler* IndexHandler::incrHandler() {
	return incr;
}

Realtime* IndexHandler::realtimeHandler() {
	return realtime;
}

TermReader* IndexHandler::fullTermReader() {
	return fullReader;
}

TermReader* IndexHandler::incrTermReader() {
	return incrReader;
}

TermReader* IndexHandler::realTermReader() {
	return realReader;
}

Handler::Handler(char *_path, IndexSchema *_schema) {
	path = _path;
	schema = _schema;
	ConjDat = NULL;
	ConjIdx = NULL;
	DnfIndexDat = NULL;
	DnfTermDat = NULL;
	DnfTermIdx = NULL;
	IndexDat = NULL;
	StoreLen = NULL;
	StorePos = NULL;
	TermDat = NULL;
	TermIdx = NULL;
	DelDat = NULL;
}

Handler::~Handler() {
	if (ConjDat != NULL) {
		free(ConjDat);
		ConjDat = NULL;
	}
	if (ConjIdx != NULL) {
		free(ConjIdx);
		ConjIdx = NULL;
	}
	if (TermIdx != NULL) {
		free(TermIdx);
		TermIdx = NULL;
	}
	if (TermDat != NULL) {
		free(TermDat);
		TermDat = NULL;
	}
	if (IndexDat != NULL) {
		free(IndexDat);
		IndexDat = NULL;
	}
	if (StoreLen != NULL) {
		free(StoreLen);
		StoreLen = NULL;
	}
	if (StorePos != NULL) {
		free(StorePos);
		StorePos = NULL;
	}
	if (DnfTermDat != NULL) {
		free(DnfTermDat);
		DnfTermDat = NULL;
	}
	if (DnfTermIdx != NULL) {
		free(DnfTermIdx);
		DnfTermIdx = NULL;
	}
	if (DnfIndexDat != NULL) {
		free(DnfIndexDat);
		DnfIndexDat = NULL;
	}
	int i = 0;
	for (i = 0; i < (int)Brifes.size(); i++) { 
		unsigned char *brife = Brifes[i];
		if (brife != NULL) {
			free(brife);
		}
		Brifes[i] = NULL;
	}
	for (i = 0; i < (int)StoreDat.size(); i++) {
		unsigned char *dat = StoreDat[i];
		if (dat != NULL) {
			free(dat);
		}
		StoreDat[i] = NULL;
	}
	if (DelDat != NULL) {
		free(DelDat);
		DelDat = NULL;
	}
	maxconjsize = 4;
}

int Handler::load(char *name, unsigned char** data, unsigned int* size) {             /* read file:name into 'data', store size of file into 'size' */
	unsigned int res = 0;
	char *buffer = NULL;
	char tpfile[MAX_DIR_LEN];
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, name);
	FILE *fp = fopen(tpfile, "r");
	if (fp == NULL) return 0;
	fseek(fp, 0l, SEEK_END);
	long filesize = ftell(fp);
	if (filesize == 0) return 0;
	int i = 0;
	for (i = 0; i < 10; i++) {
		buffer = (char*)malloc(filesize * sizeof(char));
		if (buffer != NULL) break;
	}
	if (buffer == NULL) return 0;
	*data = (unsigned char*)buffer;
	*size = (unsigned int)filesize;
	for (i = 0; i < 10; i++) {
		fseek(fp, 0l, SEEK_SET);
		res = fread(buffer, sizeof(char), *size, fp);
		if (res == *size) break;
	}
	if (res != *size) return 0;
	fclose(fp);
	return 1;
}

int Handler::init() {
	int num = 0;
	int res = 0, i = 0;
	res = load(CONJUCTION_DAT, &ConjDat, &ConjDatSize);
	if (res <= 0) return 0;
	res = load(CONJUCTION_INDEX, &ConjIdx, &ConjIdxSize);
	if (res <= 0) return 0;
	res = load(INDEX_TERM_SKIP, &TermIdx, &TermIdxSize);
	if (res <= 0) return 0;
	res = load(INDEX_TERM_DAT, &TermDat, &TermDatSize);
	if (res <= 0) return 0;
	res = load(INDEX_INVERT_DAT, &IndexDat, &IndexDatSize);
	if (res <= 0) return 0;
	res = load(STORE_LEN_FILE_NAME, &StoreLen, &StoreLenSize);
	if (res <= 0 || (StoreLenSize % 4) != 0) return 0;
	num = StoreLenSize / 4;
	res = load(STORE_POS_FILE_NAME, &StorePos, &StorePosSize);
	if (res <= 0 || ((int)StorePosSize % 8) != 0) return 0;
	if (num != (int)(StorePosSize / 8)) return 0;
	res = load(DNF_INDEX_TERM_DAT, &DnfTermDat, &DnfTermDatSize);
	if (res <= 0) return 0;
	res = load(DNF_INDEX_TERM_SKIP, &DnfTermIdx, &DnfTermIdxSize);
	if (res <= 0) return 0;
	res = load(DNF_INDEX_INVERT_DAT, &DnfIndexDat, &DnfIndexDatSize);
	if (res <= 0) return 0;
	docnum = num;
	int brifenum = schema->size();
	IndexField **fieldArray = schema->toArray();
	unsigned char *brifedata = NULL;
	unsigned int brifelen = 0;
	char tpfile[MAX_DIR_LEN];
	for (i = 0; i < brifenum; i++) {
		IndexField *get = fieldArray[i];
		if (get->isFilter() && get->getLength() > 0) {
			snprintf(tpfile, MAX_DIR_LEN, "%s.dat", get->getName());                        /* briefname.dat */
			res = load(tpfile, &brifedata, &brifelen);
			if (res <= 0) return 0;
			if ((int)brifelen != (num * get->getLength())) return 0;
		} else {
			brifedata = NULL;
		}
		Brifes.push_back(brifedata);
	}
	int storeNum = (num + 10000000 - 1) / 10000000;
	unsigned char *storedata = NULL;
	unsigned int storelen = 0;
	for (i = 0; i < storeNum; i++) {
		snprintf(tpfile, MAX_DIR_LEN, "store.dat.%d", i);
		res = load(tpfile, &storedata, &storelen);
		if (res <= 0) return 0;
		if (storelen <= 0) return 0;
		StoreDat.push_back(storedata);
		StoreDatSize.push_back(storelen);
	}
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, INDEX_DEL);
	FILE *fp = fopen(tpfile, "r");
	int excepted = (num + 7) / 8;
	if (fp == NULL) {
		for (i = 0; i < 10; i++) {
			DelDat = (unsigned char*)malloc(excepted * sizeof(char));
			if (DelDat != NULL) break;
		}
		if (DelDat == NULL) return 0;
		bzero(DelDat, excepted);
		DelDatSize = excepted;
	} else {
		fclose(fp);
		res = load(INDEX_DEL, &DelDat, &DelDatSize);                                         /* del.dat */
		if (res <= 0 || (int)DelDatSize != excepted) return 0;
	}
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, MAX_CONJSIZE);                               /* max.conjsize */
	fp = fopen(tpfile, "r");
	if (fp == NULL) return 0;
	bzero(tpfile, MAX_DIR_LEN);
	res = fread(tpfile, sizeof(char), 10, fp);
	if (res < 1) return 0;
	maxconjsize = atoi(tpfile);
	if (maxconjsize < 0) return 0;
	fclose(fp);
	return 1;
}
