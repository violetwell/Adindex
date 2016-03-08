#include "merge.h"

Merger::Merger(void *_conf, char *_outputPath, bool _termDict, int _groupNum, int _outGroup) {
	Conf_t *conf = (Conf_t*)_conf;
	outputPath = _outputPath;
    termDict = _termDict;
	groupNum = _groupNum;
	outGroup = _outGroup;
	schema = conf->schema;
	int num = schema->size(), i = 0;
	indexFieldNumber = 0;
	indexField = (IndexField**)malloc(sizeof(IndexField*) * num);
	storeFieldNumber = 0;
	storeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	brifeFieldNumber = 0;
    brifeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	IndexField **fieldArray = schema->toArray();
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isIndex()) {
			indexField[indexFieldNumber++] = get;
		}
		if (get->isStore()) {
			storeField[storeFieldNumber++] = get;
		}
		if (get->isFilter() && get->getLength() > 0) {                                    /* fix length */
			brifeField[brifeFieldNumber++] = get;
		}
	}
	docNums = (int*)malloc(sizeof(int) * groupNum);
	bzero(docNums, sizeof(int) * groupNum);
	samenum = 0;
	termNums = (int*)malloc(sizeof(int) * groupNum);
	mergeinfo = (MergeInfo*)malloc(sizeof(MergeInfo) * groupNum);
	loadTerms = (TermDat**)malloc(sizeof(TermDat*) * groupNum);
	for (i = 0; i < groupNum; i++) {
		loadTerms[i] = (TermDat*)malloc(sizeof(TermDat) * LOAD_TERM_NUM);
	}
	readedTermNum= (int*)malloc(sizeof(int) * groupNum);
	bzero(readedTermNum, sizeof(int) * groupNum);
	termReaders = (BufferedReader**)malloc(sizeof(BufferedReader*) * groupNum);
	bzero(termReaders, sizeof(BufferedReader*) * groupNum);
	indexReaders = (BufferedReader**)malloc(sizeof(BufferedReader*) * groupNum);
	bzero(indexReaders, sizeof(BufferedReader*) * groupNum);
	termBuffer = (char**)malloc(sizeof(char*) * groupNum);
	for (i = 0; i < groupNum; i++) {
		termBuffer[i] = (char*)malloc(sizeof(char) * LOAD_TERM_NUM * MAX_TERM_LENGTH);
	}
	termFp = NULL;
	indexFp = NULL;
	termSkipFp = NULL;
	termWriteBuffer = (char*)malloc(sizeof(char) * 200);
	writeBuffer = (char*)malloc(sizeof(char) * 102400);
	termWriteLen = 0;
}

Merger::~Merger() {
	if (indexField != NULL) {
		free(indexField);
		indexField = NULL;
	}
	if (storeField != NULL) {
		free(storeField);
		storeField = NULL;
	}
	if (brifeField != NULL) {
		free(brifeField);
		brifeField = NULL;
	}
	if (docNums != NULL) {
		free(docNums);
		docNums = NULL;
	}
	if (mergeinfo != NULL) {
		free(mergeinfo);
		mergeinfo = NULL;
	}
	if (termNums != NULL) {
		free(termNums);
		termNums = NULL;
	}
	if (readedTermNum != NULL) {
		free(readedTermNum);
		readedTermNum = NULL;
	}
	int i = 0;
	if (termBuffer != NULL) {
		for (i = 0; i < groupNum; i++) {
			char *term = termBuffer[i];
			if (term != NULL) free(term);
			termBuffer[i] = NULL;
		}
		free(termBuffer);
		termBuffer = NULL;
	}
	if (loadTerms != NULL) {
		for (i = 0; i < groupNum; i++) {
			TermDat *buffer = loadTerms[i];
			if (buffer != NULL) free(buffer);
			loadTerms[i] = NULL;
		}
		free(loadTerms);
		loadTerms = NULL;
	}
	if (termReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *reader = termReaders[i];
			if (reader != NULL) delete reader;
			termReaders[i] = NULL;
		}
		free(termReaders);
		termReaders = NULL;
	}
	if (indexReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *indexreader = indexReaders[i];
			if (indexreader != NULL) delete indexreader;
			indexReaders[i] = NULL;
		}
		free(indexReaders);
		indexReaders = NULL;
	}
	if (termFp != NULL) {
		fclose(termFp);
		termFp = NULL;
	}
	if (indexFp != NULL) {
		fclose(indexFp);
		indexFp = NULL;
	}
	if (termSkipFp != NULL) {
		fclose(termSkipFp);
		termSkipFp = NULL;
	}
	if (writeBuffer != NULL) {
		free(writeBuffer);
		writeBuffer = NULL;
	}
	if (termWriteBuffer != NULL) {
		free(termWriteBuffer);
		termWriteBuffer = NULL;
	}
}

int Merger::merge() {
	int res = 0;
	res = init();
	if (res <= 0) {
		return 0;
	}
	res = brife();
	if (res <= 0) {
		return 0;
	}
	res = store();
	if (res <= 0) {
		return 0;
	}
	res = inverted();
	if (res <= 0) {
		return 0;
	}
	res = dnfInverted();
	if (res <= 0) {
		return 0;
	}
	res = over();
	return res;
}

int Merger::dnfInverted() {
	char tpfile[MAX_DIR_LEN];
	int curmin = 0, res = 0, i = 0;
	TermDat *minterm = NULL, *curterm = NULL;
	if (outGroup >= 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, DNF_INDEX_TERM_DAT, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, DNF_INDEX_TERM_DAT);                            /* open dnfterm.dat in fullindex/merger */
	}
	termFp = fopen(tpfile, "w");
	if (termFp == NULL) return 0;
	bzero(tpfile, sizeof(char) * 8);
	res = fwrite(tpfile, sizeof(char), 8, termFp);
	if (res != 8) return 0;
	if (outGroup >= 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, DNF_INDEX_INVERT_DAT, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, DNF_INDEX_INVERT_DAT);                           /* open dnfindex.dat in fullindex/merger */
	}
	indexFp = fopen(tpfile, "w");
	if (indexFp == NULL) return 0;
	if (termDict) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, DNF_INDEX_TERM_SKIP);                            /* open dnfterm.idx in fullindex/merger */
		termSkipFp = fopen(tpfile, "w");
		if (termSkipFp == NULL) return 0;
		bzero(tpfile, sizeof(char) * 8);
		res = fwrite(tpfile, sizeof(char), 8, termSkipFp);
		if (res != 8) return 0;
	}
	bzero(termNums, sizeof(int) * groupNum);
	char *readbuffer = (char*)malloc(sizeof(char) * 8);
	for (i = 0; i < groupNum; i++) {
		inpath(tpfile, DNF_INDEX_TERM_DAT, i);                                                             /* open dnfterm.dat.groupNum in index / open dnfterm.dat in incr and realtime */
		FILE *fp = fopen(tpfile, "r");
		if (fp == NULL) {
			return 0;
		}
		res = fread(readbuffer, sizeof(char), 8, fp);
		if (res != 8) {
			return 0;
		}
		termNums[i] = (readbuffer[4] & 0xff) + ((readbuffer[5] & 0xff) << 8)                               /* get term count of each group */
		+ ((readbuffer[6] & 0xff) << 16) + ((readbuffer[7] & 0xff) << 24);
		if (termNums[i] < 0) {
			return 0;
		}
		termReaders[i] = new BufferedReader(fp, 0);
		inpath(tpfile, DNF_INDEX_INVERT_DAT, i);                                                           /* open dnfindex.dat.groupNum in index / open dnfindex.dat in incr and realtime */
		fp = fopen(tpfile, "r");
		if (fp == NULL) {
			return 0;
		}
		indexReaders[i] = new BufferedReader(fp, 0);
	}
	free(readbuffer);
	bzero(mergeinfo, sizeof(MergeInfo) * groupNum);
	bzero(readedTermNum, sizeof(int) * groupNum);
	termWrite = 0;
	indexWrite = 0;
	mergedNum = 0;
	while (1) {
		curmin = -1;
		for (i = 0; i < groupNum; i++) {
			if (mergeinfo[i].cur < termNums[i]) {
				curmin = i;
				break;
			}
		}
		if (curmin < 0) break;
		samenum = 0;
		for (i = 0; i < groupNum; i++) {
			if (mergeinfo[i].cur >= termNums[i]) continue;
			minterm = getDnfTerm(curmin, mergeinfo[curmin].cur);
			curterm = getDnfTerm(i, mergeinfo[i].cur);
			if (minterm == NULL || curterm == NULL) {
				return -1;
			}
			res = termCmp(minterm, curterm);
			if (res == 0) {
				mergeinfo[samenum].id = i;
				samenum++;
			} else if (res > 0) {
				curmin = i;
				samenum = 0;
				mergeinfo[samenum].id = i;
				samenum++;
			}
		}
		if (mergeDnfTermIndex() <= 0) {
			return -1;
		}
		for (int i = 0; i < samenum; i++) {
			mergeinfo[mergeinfo[i].id].cur++;
		}
	}
	if (termDict) {
		/*
		vlong = termWrite - lastWrite;
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 24);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 32);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 40);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 48);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 56);
		*/
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termSkipFp);
		if (res != termWriteLen) return 0;
	}
	fseek(termFp, 0l, SEEK_SET);
	termWriteBuffer[0] = (char)(1 >> 0);
	termWriteBuffer[1] = (char)(1 >> 8);
	termWriteBuffer[2] = (char)(1 >> 16);
	termWriteBuffer[3] = (char)(1 >> 24);
	termWriteBuffer[4] = (char)(mergedNum >> 0);
	termWriteBuffer[5] = (char)(mergedNum >> 8);
	termWriteBuffer[6] = (char)(mergedNum >> 16);
	termWriteBuffer[7] = (char)(mergedNum >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termFp);
	if (res != 8) return 0;
	if (termDict) {
	    fseek(termSkipFp, 0l, SEEK_SET);
	    mergedNum = (mergedNum + 127) / 128 + 1;
	    termWriteBuffer[4] = (char)(mergedNum >> 0);
	    termWriteBuffer[5] = (char)(mergedNum >> 8);
	    termWriteBuffer[6] = (char)(mergedNum >> 16);
	    termWriteBuffer[7] = (char)(mergedNum >> 24);
	    res = fwrite(termWriteBuffer, sizeof(char), 8, termSkipFp);
	    if (res != 8) return 0;
	}
	if (termFp != NULL) {
		fclose(termFp);
		termFp = NULL;
	}
	if (indexFp != NULL) {
		fclose(indexFp);
		indexFp = NULL;
	}
	if (termSkipFp != NULL) {
		fclose(termSkipFp);
		termSkipFp = NULL;
	}
	if (termReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *reader = termReaders[i];
			if (reader != NULL) {
				reader->close();
				delete reader;
			}
			termReaders[i] = NULL;
		}
	}
	if (indexReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *indexreader = indexReaders[i];
			if (indexreader != NULL) {
				indexreader->close();
				delete indexreader;
			}
			indexReaders[i] = NULL;
		}
	}
	return 1;
}

TermDat* Merger::getDnfTerm(int group, int termno) {
	int readnum = 0, i = 0, termBufferOff = 0;
	int termLength = 0, docNum = 0, res = 0;
	long indexOff = 0;
	if (termno >= readedTermNum[group]) {
		bzero(loadTerms[group], sizeof(TermDat) * LOAD_TERM_NUM);
		bzero(termBuffer[group], sizeof(char) * LOAD_TERM_NUM * MAX_TERM_LENGTH);
		if (termNums[group] - readedTermNum[group] <= LOAD_TERM_NUM) {
			readnum = termNums[group] - readedTermNum[group];
		} else {
			readnum = LOAD_TERM_NUM;
		}
		for (i = 0; i < readnum; i++) {
			termLength = 9;                                                                    /* conjSize(1byte) + name(2byte) + value(6byte) */
			res = termReaders[group]->read(termBuffer[group], termBufferOff, termLength);
			if (res != termLength) return NULL;
			docNum = termReaders[group]->readInt();
			indexOff = termReaders[group]->readLong();
			if (docNum < 0 || indexOff < 0) return NULL;
			loadTerms[group][i].data = termBuffer[group] + termBufferOff;
			loadTerms[group][i].dataLen = termLength;
			loadTerms[group][i].docNum = docNum;
			loadTerms[group][i].offset = indexOff;
			termBufferOff += termLength;
		}
		readedTermNum[group] += readnum;
	}
	return loadTerms[group] + (termno % LOAD_TERM_NUM);
}

int Merger::termCmp(TermDat *a, TermDat *b) {
	char *text1 = a->data, *text2 = b->data;
	int len1 = a->dataLen, len2 = b->dataLen;
	if (len1 > len2) {
		int res = memcmp(text2, text1, len2);
		if (res > 0) return -1;
		else if (res == 0) return 1;
	    else return 1;
	} else if (len1 == len2) {
		int res = memcmp(text2, text1, len2);
		if (res > 0) return -1;
		else if (res == 0) return 0;
	    else return 1;
	} else {
		int res = memcmp(text1, text2, len1);
		if (res > 0) return 1;
		else if (res == 0) return -1;
	    else return -1;
	}
}

int Merger::inverted() {
	char tpfile[MAX_DIR_LEN];
	int curmin = 0, res = 0, i = 0;
	long vlong = 0;
	bzero(termNums, sizeof(int) * groupNum);
	TermDat *minterm = NULL, *curterm = NULL;
	if (outGroup >= 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, INDEX_TERM_DAT, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, INDEX_TERM_DAT);                              /* open term.dat in fullindex/merger */
	}
	termFp = fopen(tpfile, "w");
	if (termFp == NULL) return 0;
	bzero(tpfile, sizeof(char) * 8);
	res = fwrite(tpfile, sizeof(char), 8, termFp);                                                      /* write 0(8byte) into term.dat */
	if (res != 8) return 0;
	if (outGroup >= 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, INDEX_INVERT_DAT, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, INDEX_INVERT_DAT);                            /* open index.dat in fullindex/merger */
	}
	indexFp = fopen(tpfile, "w");
	if (indexFp == NULL) return 0;
	if (termDict) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, INDEX_TERM_SKIP);                             /* open term.idx in fullindex/merger */
		termSkipFp = fopen(tpfile, "w");
		if (termSkipFp == NULL) return 0;
		bzero(tpfile, sizeof(char) * 8);
		res = fwrite(tpfile, sizeof(char), 8, termSkipFp);                                              /* write 0(8byte) into term.idx */
		if (res != 8) return 0;
	}
	char *readbuffer = (char*)malloc(sizeof(char) * 8);
	for (i = 0; i < groupNum; i++) {
		inpath(tpfile, INDEX_TERM_DAT, i);                                                              /* open term.dat.groupNum in index / open term.dat in incr and realtime */
		FILE *fp = fopen(tpfile, "r");
		if (fp == NULL) {
			return 0;
		}
		res = fread(readbuffer, sizeof(char), 8, fp);
		if (res != 8) {
			return 0;
		}
		termNums[i] = (readbuffer[4] & 0xff) + ((readbuffer[5] & 0xff) << 8)                            /* get term count of each group */
		+ ((readbuffer[6] & 0xff) << 16) + ((readbuffer[7] & 0xff) << 24);
		if (termNums[i] < 0) return 0;
		termReaders[i] = new BufferedReader(fp, 0);
		inpath(tpfile, INDEX_INVERT_DAT, i);                                                            /* open index.dat.groupNum in index/(incr or realtime) */
		fp = fopen(tpfile, "r");
		if (fp == NULL) {
			return 0;
		}
		indexReaders[i] = new BufferedReader(fp, 0);
	}
	free(readbuffer);
	bzero(mergeinfo, sizeof(MergeInfo) * groupNum);
	bzero(readedTermNum, sizeof(int) * groupNum);
	termWrite = 0;
	indexWrite = 0;
	mergedNum = 0;
	lastWrite = 0;
	while (1) {                                                               /* write all term into term.idx, term.dat, index.dat in increasing order, delete multiple terms in all group */
		curmin = -1;
		for (i = 0; i < groupNum; i++) {                                      /* find the first group that is not fully written */
			if (mergeinfo[i].cur < termNums[i]) {                             /* mergeinfo[i].cur: the number of term to be written */
				curmin = i;
				break;
			}
		}
		if (curmin < 0) break;
		samenum = 0;
		for(i = 0; i < groupNum; i++) {                                        /* get the smallest term to be written */
			if (mergeinfo[i].cur >= termNums[i]) continue;
			minterm = getTerm(curmin, mergeinfo[curmin].cur);
			curterm = getTerm(i, mergeinfo[i].cur);
			if (minterm == NULL || curterm == NULL) {
				return -1;
			}
			res = termCmp(minterm, curterm);
			if (res == 0) {
				mergeinfo[samenum].id = i;                                     /* mark the groupId of which the term is equal to minterm */
				samenum++;
			} else if (res > 0) {
				curmin = i;
				samenum = 0;
				mergeinfo[samenum].id = i;                                     /* find a smaller term, mark the groupId */
				samenum++;
			}
		}
		if (mergeTermIndex() <= 0) {
			return -1;
		}
		for (int i = 0; i < samenum; i++) {                                    /* one term written, move the written groups(might be multiple) to the next term */
			mergeinfo[mergeinfo[i].id].cur++;
		}
	}
	if (termDict) {                                                            /* write the last term of term.dat into term.idx */
		vlong = termWrite - lastWrite;
		while ((vlong & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f) | 0x80);
			vlong >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f));
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termSkipFp);
		if (res != termWriteLen) {
			return 0;
		}
	}
	fseek(termFp, 0l, SEEK_SET);                                                /* write version and term count into term.dat */
	termWriteBuffer[0] = (char)(1 >> 0);
	termWriteBuffer[1] = (char)(1 >> 8);
	termWriteBuffer[2] = (char)(1 >> 16);
	termWriteBuffer[3] = (char)(1 >> 24);
	termWriteBuffer[4] = (char)(mergedNum >> 0);
	termWriteBuffer[5] = (char)(mergedNum >> 8);
	termWriteBuffer[6] = (char)(mergedNum >> 16);
	termWriteBuffer[7] = (char)(mergedNum >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termFp);
	if (res != 8) {
		return 0;
	}
	if (termDict) {                                                             /* write version and term count into term.idx */
	    fseek(termSkipFp, 0l, SEEK_SET);
	    mergedNum = (mergedNum + 127) / 128 + 1;
	    termWriteBuffer[4] = (char)(mergedNum >> 0);
	    termWriteBuffer[5] = (char)(mergedNum >> 8);
	    termWriteBuffer[6] = (char)(mergedNum >> 16);
	    termWriteBuffer[7] = (char)(mergedNum >> 24);
	    res = fwrite(termWriteBuffer, sizeof(char), 8, termSkipFp);
	    if (res != 8) {
			return 0;
		}
	}
	if (termFp != NULL) {
		fclose(termFp);
		termFp = NULL;
	}
	if (indexFp != NULL) {
		fclose(indexFp);
		indexFp = NULL;
	}
	if (termSkipFp != NULL) {
		fclose(termSkipFp);
		termSkipFp = NULL;
	}
	if (termReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *reader = termReaders[i];
			if (reader != NULL) {
				reader->close();
				delete reader;
			}
			termReaders[i] = NULL;
		}
	}
	if (indexReaders != NULL) {
		for (i = 0; i < groupNum; i++) {
			BufferedReader *indexreader = indexReaders[i];
			if (indexreader != NULL) {
				indexreader->close();
				delete indexreader;
			}
			indexReaders[i] = NULL;
		}
	}
	return 1;
}

TermDat* Merger::getTerm(int group, int termno) {                                          /* return the termno-th term in group */
	int readnum = 0, i = 0, termBufferOff = 0;
	int termLength = 0, docNum = 0, res = 0;
	long indexOff = 0;
	float ub = 0;
	if (termno >= readedTermNum[group]) {                                                  /* buffer is finished, load new terms of group in size of LOAD_TERM_NUM */
		bzero(loadTerms[group], sizeof(TermDat) * LOAD_TERM_NUM);
		bzero(termBuffer[group], sizeof(char) * LOAD_TERM_NUM * MAX_TERM_LENGTH);
		if (termNums[group] - readedTermNum[group] <= LOAD_TERM_NUM) {
			readnum = termNums[group] - readedTermNum[group];
		} else {
			readnum = LOAD_TERM_NUM;
		}
		for (i = 0; i < readnum; i++) {
			termLength = termReaders[group]->readVInt();
			if (termLength <= 0) return NULL;
			res = termReaders[group]->read(termBuffer[group], termBufferOff, termLength);  /* buffer the term name into termBuffer */
			if (res != termLength) return NULL;
			docNum = termReaders[group]->readVInt();
			indexOff = termReaders[group]->readVLong();
			ub = termReaders[group]->readUb();
			if (docNum < 0 || indexOff < 0 || ub < 0) return NULL;
			loadTerms[group][i].data = termBuffer[group] + termBufferOff;                  /* buffer the term data into loadTerms */
			loadTerms[group][i].dataLen = termLength;
			loadTerms[group][i].docNum = docNum;
			loadTerms[group][i].offset = indexOff;
			loadTerms[group][i].ub = ub;
			termBufferOff += termLength;
		}
		readedTermNum[group] += readnum;
	}
	return loadTerms[group] + (termno % LOAD_TERM_NUM);
}

int Merger::mergeTermIndex() {                                                      /* write term */
	int objno = 0, indexnum = 0;
	int mergednum = 0, writeNum = 0, i = 0;
	int res = 0, vint = 0;
	long vlong = 0;
	float ub = 0;
	//termWriteLen = 0;
	TermDat *sortterm = NULL;
	long lastindexWrite = indexWrite;
	for (i = 0; i < groupNum; i++) {                                                /* write index.dat, merge data of identical term */
		if (mergednum < samenum) {
			if (i == mergeinfo[mergednum].id) {
				sortterm = getTerm(i, mergeinfo[i].cur);
				if (sortterm->ub > ub) ub = sortterm->ub;                           /* mark UB */
				writeNum = writeIndex(objno, i, sortterm->docNum);                  /* write index.dat */
				indexnum += writeNum;
				mergednum++;
			}
		}
		objno += docNums[i];
	}
	if (indexnum > 0) {
		termWriteLen = 0;
		vint = sortterm->dataLen;
		while ((vint & ~0x7F) != 0) {                                                /* term length */
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7F) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		memcpy(termWriteBuffer + termWriteLen, sortterm->data, sortterm->dataLen);
		termWriteLen += sortterm->dataLen;
		vint = indexnum;                                                             /* doc count */
		while ((vint & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		vlong = lastindexWrite;                                                      /* doc shift */
		while ((vlong & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f) | 0x80);
			vlong >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f));
		Utils::decode(ub, termWriteBuffer + termWriteLen);                           /* UB */
		termWriteLen += 2;
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termFp);           /* write term.dat in fullindex/merger */
		if (res != termWriteLen) return 0;
		int temp = termWriteLen;
		if (termDict && mergedNum % 128 == 0) {
			vlong = termWrite;
			while ((vlong & ~0x7F) != 0) {
				termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f) | 0x80);
				vlong >>= 7;
			}
			termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f));
			res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termSkipFp);   /* write term.idx in fullindex/merger */
			if (res != termWriteLen) return 0;
		}
		termWrite += temp;
		lastWrite = temp;
		mergedNum++;
	}
	return 1;
}

int Merger::mergeDnfTermIndex() {
	int indexnum = 0;
	int mergednum = 0, writeNum = 0, i = 0;
	int res = 0, vint = 0;
	long vlong = 0;
	//termWriteLen = 0;
	TermDat *sortterm = NULL;
	long lastindexWrite = indexWrite;
	for (i = 0; i < groupNum; i++) {
		if (mergednum < samenum) {
			if (i == mergeinfo[mergednum].id) {
				sortterm = getTerm(i, mergeinfo[i].cur);                                  /* TODO: getDnfTerm */
				writeNum = writeDnfIndex(i, sortterm->docNum);                            /* write dnfindex.dat */
				indexnum += writeNum;
				mergednum++;
			}
		}
	}
	if (indexnum > 0) {
		termWriteLen = 0;
		memcpy(termWriteBuffer + termWriteLen, sortterm->data, sortterm->dataLen);         /* dnf term data */
		termWriteLen += sortterm->dataLen;
		vint = indexnum;                                                                   /* conj count, 4bytes */
		termWriteBuffer[termWriteLen++] = (char)(vint >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 24);
		vlong = lastindexWrite;                                                            /* conj shift, 8bytes */
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 24);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 32);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 40);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 48);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 56);
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termFp);                  /* write dnfterm.dat */
		if (res != termWriteLen) return 0;
		int temp = termWriteLen;
		if (termDict && mergedNum % 128 == 0) {
			/*
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 0);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 8);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 16);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 24);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 32);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 40);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 48);
			termWriteBuffer[termWriteLen++] = (char)(vlong >> 56);
			*/
			res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termSkipFp);          /* write dnfterm.idx */
			if (res != termWriteLen) return 0;
		}
		termWrite += temp;
		mergedNum++;
	}
	return 1;
}

FullMerger::FullMerger(void *_conf, char *_outputPath, bool _termDict, 
char *_conjPath, char *_inputPath, int *_groups, int _groupNum, int _outGroup)
: Merger(_conf, _outputPath, _termDict, _groupNum, _outGroup) {
	inputPath = _inputPath;
	conjPath = _conjPath;
	groups = _groups;
}

FullMerger::~FullMerger() {
}

void FullMerger::inpath(char *tp, char* name, int group) {
	snprintf(tp, MAX_DIR_LEN, "%s%s.%d", inputPath, name, groups[group]);
}

int FullMerger::init() {                                                                           /* get docNums, check file */
	int i = 0, j = 0, num = 0;
	char tpfile[MAX_DIR_LEN];
	FILE *fp = NULL;
	long filesize = 0;
	for (i = 0; i < groupNum; i++) {
		num = 0;
		for (j = 0; j < brifeFieldNumber; j++) {
			IndexField *field = brifeField[j];
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat.%d", inputPath, field->getName(), groups[i]);   /* fieldName.dat.groupNumber */
			fp = fopen(tpfile, "r");
			if (fp == NULL) return 0;
			fseek(fp, 0l, SEEK_END);
			filesize = ftell(fp);
			fclose(fp);
			if (num == 0) num = (int)(filesize / field->getLength());                               /* get doc count */
			else {
				if (num != (int)(filesize / field->getLength())) return 0;                          /* check doc count of all brief file */
			}
		}
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", inputPath, STORE_POS_FILE_NAME, groups[i]);        /* check store.pos.groupNumber */
		fp = fopen(tpfile, "r");
		if (fp == NULL) return 0;
		fseek(fp, 0l, SEEK_END);
		filesize = ftell(fp);
		fclose(fp);
		if (num != (int)(filesize / 8)) return 0;
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", inputPath, STORE_LEN_FILE_NAME, groups[i]);        /* check store.len.groupNumber */
		fp = fopen(tpfile, "r");
		if (fp == NULL) return 0;
		fseek(fp, 0l, SEEK_END);
		filesize = ftell(fp);
		fclose(fp);
		if (num != (int)(filesize / 4)) return 0;
		docNums[i] = num;
	}
	return 1;
}

int FullMerger::over() {
	if (termDict) {
		int res = 0;
		char srcfile[MAX_DIR_LEN];
		char destfile[MAX_DIR_LEN];
	    snprintf(srcfile, MAX_DIR_LEN, "%s%s", inputPath, CONJUCTION_INDEX);        /* copy conj.idx */
	    snprintf(destfile, MAX_DIR_LEN, "%s%s", outputPath, CONJUCTION_INDEX);
	    res = Utils::copyFile(srcfile, destfile);
		if (res <= 0) return 0;
	    snprintf(srcfile, MAX_DIR_LEN, "%s%s", inputPath, CONJUCTION_DAT);          /* copy conj.dat */
	    snprintf(destfile, MAX_DIR_LEN, "%s%s", outputPath, CONJUCTION_DAT);
	    res = Utils::copyFile(srcfile, destfile);
		if (res <= 0) return 0;
		snprintf(srcfile, MAX_DIR_LEN, "%s%s", inputPath, MAX_CONJSIZE);            /* copy max.conjsize */
	    snprintf(destfile, MAX_DIR_LEN, "%s%s", outputPath, MAX_CONJSIZE);
	    res = Utils::copyFile(srcfile, destfile);
		if (res <= 0) return 0;
	}
	return 1;
}

int FullMerger::writeIndex(int objno, int group, int num) {                           /* write the index data of current term of group */
	int i = 0, res = 0, writeLen = 0;
	for (i = 0; i < num; i++) {
		if (writeLen >= (102400 - 6)) {
			indexWrite += writeLen;
			res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);
			if (res != writeLen) return -1;
			writeLen = 0;
		}
		int docid = indexReaders[group]->readInt();
		float weight = indexReaders[group]->readUb();
		docid += objno;                                                               /* since docId counts from 0 in each group, we need to add shift for fullindex */
		writeBuffer[writeLen++] = (char)(docid >> 0);
		writeBuffer[writeLen++] = (char)(docid >> 8);
		writeBuffer[writeLen++] = (char)(docid >> 16);
		writeBuffer[writeLen++] = (char)(docid >> 24);
		Utils::decode(weight, writeBuffer + writeLen);
		writeLen += 2;
	}
	if (writeLen > 0) {
		indexWrite += writeLen;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);
		if (res != writeLen) return -1;
	}
	return num;
}

int FullMerger::writeDnfIndex(int group, int num) {
	int i = 0, res = 0, writeLen = 0;
	for (i = 0; i < num; i++) {
		if (writeLen >= (102400 - 6)) {
			indexWrite += writeLen;
			res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);         /* write dnfindex.dat */
			if (res != writeLen) return -1;
			writeLen = 0;
		}
		int docid = indexReaders[group]->readInt();
		int belong = indexReaders[group]->read();
		writeBuffer[writeLen++] = (char)(docid >> 0);                           /* conjId */
		writeBuffer[writeLen++] = (char)(docid >> 8);
		writeBuffer[writeLen++] = (char)(docid >> 16);
		writeBuffer[writeLen++] = (char)(docid >> 24);
		writeBuffer[writeLen++] = (char)belong;                                 /* belong */
	}
	if (writeLen > 0) {
		indexWrite += writeLen;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);
		if (res != writeLen) return -1;
	}
	return num;
}

int FullMerger::brife() {                                                        /* write brief file: fieldName.dat.0-fieldName.dat.n into fieldName.dat in fullindex */
	char tpfile[MAX_DIR_LEN];
	int i = 0, j = 0, res = 0;
	bool flag = true;
	FILE **brifeFps = (FILE**)malloc(sizeof(FILE*) * groupNum);
	FILE *outFp = NULL;
	bzero(brifeFps, sizeof(FILE*) * groupNum);
	for (i = 0; (i < brifeFieldNumber) && flag; i++) {                           /* for each brief field */
		IndexField *field = brifeField[i];
		for (j = 0; j < groupNum; j++) {
			FILE *brifeFp = brifeFps[j];
			if (brifeFp != NULL) fclose(brifeFp);
			brifeFps[j] = NULL;
		}
		if (outFp != NULL) {
			fclose(outFp);
			outFp = NULL;
		}
		if (outGroup >= 0) {
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat.%d", outputPath, field->getName(), outGroup);
		} else {
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat", outputPath, field->getName());                /* open fieldName.dat in fullindex */
		}
		outFp = fopen(tpfile, "w");
		if (outFp == NULL) {
			flag = false;
			break;
		}
		for (j = 0; j < groupNum; j++) {
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat.%d", inputPath, field->getName(), groups[j]);   /* open fieldName.dat.0-fieldName.dat.n in index */
			brifeFps[j] = fopen(tpfile, "r");
			if (brifeFps[j] == NULL) {
				flag = false;
				break;
			}
		}
		char *buffer = (char*)malloc(1024 * field->getLength());
		int toRead = 0;
		for (j = 0; (j < groupNum) && flag; j++) {                                                  /* for each groupNum */
			int fileLen = docNums[j];
			while (fileLen > 0) {
				if (fileLen >= 1024) toRead = 1024;
				else toRead = fileLen;
				res = fread(buffer, field->getLength(), toRead, brifeFps[j]);                       /* read fieldName.dat.groupNum */
				if (res != toRead) {
					flag = false;
					break;
				}
				res = fwrite(buffer, field->getLength(), toRead, outFp);                            /* write fieldName.dat in fullindex */
				if (res != toRead) {
					flag = false;
					break;
				}
                fileLen -= toRead;
			}
			fclose(brifeFps[j]);
			brifeFps[j] = NULL;
		}
		fclose(outFp);
		outFp = NULL;
		free(buffer);
	}
	if (brifeFps != NULL) {
		for (i = 0; i < groupNum; i++) {
			FILE *brifeFp = brifeFps[i];
			if (brifeFp != NULL) fclose(brifeFp);
			brifeFps[i] = NULL;
		}
		free(brifeFps);
	}
	if (outFp != NULL) {
		fclose(outFp);
		outFp = NULL;
	}
	return flag ? 1 : 0;
}

int FullMerger::store() {
	char tpfile[MAX_DIR_LEN];
	FILE *lenFp = NULL, *posFp = NULL, *srcfp = NULL, *destfp = NULL;
	int i = 0, res = 0, readednum = 0, objnum = 0, rnum = 0, obj_no = 0;
	int srcfno = -1, srcobjoff = 0, srcobjleft = 0, obj_id = 0;
	int destfno = -1, destobjoff = 0, destobjleft = 0, curlen = 0, readlen = 0, writelen = 0;
	bool flag = true;
	long curpos = 0;
	char *srcbuf = NULL;
	if (outGroup >= 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, STORE_POS_FILE_NAME, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, STORE_POS_FILE_NAME);                       /* open store.pos in fullindex */
	}
	posFp = fopen(tpfile, "w");
	if (posFp == NULL) flag = false;
	if (outGroup >= 0) {
	    snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, STORE_LEN_FILE_NAME, outGroup);
	} else {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, STORE_LEN_FILE_NAME);                        /* open store.len in fullindex */
	}
	lenFp = fopen(tpfile, "w");
	if (lenFp == NULL) flag = false;
	Store_Pos *pos_buf = NULL;
	Store_Length *length_buf = NULL;
	pos_buf = (Store_Pos*)malloc(MAX_STORE_UNIT * sizeof(Store_Pos));
	bzero(pos_buf, MAX_STORE_UNIT * sizeof(Store_Pos));
	length_buf = (Store_Length*)malloc(MAX_STORE_UNIT * sizeof(Store_Length));
	bzero(length_buf, MAX_STORE_UNIT * sizeof(Store_Length));
	Store_Pos **posarr = (Store_Pos**)malloc(sizeof(Store_Pos*) * groupNum);
	bzero(posarr, sizeof(Store_Pos*) * groupNum);
	Store_Length **lengtharr = (Store_Length**)malloc(sizeof(Store_Length*) * groupNum);
	bzero(lengtharr, sizeof(Store_Length*) * groupNum);
	srcbuf = (char*)malloc(sizeof(char) * MAX_STORE_UNIT * MAX_STORE_LENGTH);
	for (i = 0; (i < groupNum) && flag; i++) {                                                         /* for each groupNum */
		FILE *sublenFp = NULL, *subposFp = NULL;
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", inputPath, STORE_POS_FILE_NAME, groups[i]);
		subposFp = fopen(tpfile, "r");                                                                 /* open store.pos.groupNum in index */
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", inputPath, STORE_LEN_FILE_NAME, groups[i]);
		sublenFp = fopen(tpfile, "r");                                                                 /* open store.len.groupNum in index */
		if (subposFp == NULL || sublenFp == NULL) {
			flag = false;
			break;
		}
		posarr[i] = (Store_Pos*)malloc(sizeof(Store_Pos) * docNums[i]);
		lengtharr[i] = (Store_Length*)malloc(sizeof(Store_Length) * docNums[i]);
		if (lengtharr[i] == NULL || posarr[i] == NULL) {
			flag = false;
			break;
		}
		res = fread(posarr[i], sizeof(Store_Pos), docNums[i], subposFp);                               /* read store.pos.groupNum in index */
		if (res != docNums[i]) {
			flag = false;
		}
		res = fread(lengtharr[i], sizeof(Store_Length), docNums[i], sublenFp);                         /* read store.len.groupNum in index */
		if (res != docNums[i]) {
			flag = false;
		}
		if (subposFp != NULL) {
			fclose(subposFp);
			subposFp = NULL;
		}
		if (sublenFp != NULL) {
			fclose(sublenFp);
			sublenFp = NULL;
		}
	}
	for (i = 0; (i < groupNum) && flag; i++) {
		objnum = docNums[i];
		readednum = 0;
		srcfno = -1;
		srcfp = NULL;
		while (readednum < objnum) {
			if (objnum - readednum < MAX_STORE_UNIT) rnum = objnum - readednum;
			else rnum = MAX_STORE_UNIT;
			int pre = srcfno, preout = destfno;
			srcfno = readednum / MAX_STORE_DOCNUM;                                                                   /* TODO: readednum < indexmaxdoc, so srcfno === 0 */
			srcobjoff = readednum % MAX_STORE_DOCNUM;
			srcobjleft = MAX_STORE_DOCNUM - srcobjoff;
			rnum = rnum < srcobjleft ? rnum : srcobjleft;
			destfno = obj_no / MAX_STORE_DOCNUM;                                                                     /* write a new file for every MAX_STORE_DOCNUM docs */
			destobjoff = obj_no % MAX_STORE_DOCNUM;
			destobjleft = MAX_STORE_DOCNUM - destobjoff;
			rnum = rnum < destobjleft ? rnum : destobjleft;
			if (pre != srcfno) {
				if (srcfp != NULL) fclose(srcfp);
				snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d.%d", inputPath, STORE_FILE_NAME, groups[i], srcfno);          /* open store.dat.groupNum.0 in index */
				srcfp = fopen(tpfile, "r");
				if (srcfp == NULL) {
					flag = false;
					break;
				}
			}
			if (preout != destfno) {
				if (destfp != NULL) fclose(destfp);
				if (outGroup > 0) {
				    snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d.%d", outputPath, STORE_FILE_NAME, outGroup, destfno);
				} else {
					snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, STORE_FILE_NAME, destfno);                  /* open store.dat.fileNumber in fullindex */
				}
				destfp = fopen(tpfile, "w");
				if (destfp == NULL) {
					flag = false;
					break;
				}				
			}
			fseek(srcfp, posarr[i][readednum].pos, SEEK_SET);                                                         /* set srcfp to last read postion */
			curpos = ftell(destfp);
			readlen = posarr[i][readednum + rnum - 1].pos - posarr[i][readednum].pos 
			+ lengtharr[i][readednum + rnum - 1].length;
			curlen = 0;
			for (obj_id = 0; obj_id < rnum; obj_id++) {
				pos_buf[obj_id].pos = curpos + curlen;                                                                /* pos */
				length_buf[obj_id].length = lengtharr[i][obj_id + readednum].length;                                  /* len */
				curlen += lengtharr[i][obj_id + readednum].length;
			}
			if (readlen != curlen) {
				flag = false;
				break;
			}
			res = fwrite(pos_buf, sizeof(Store_Pos), rnum, posFp);                                                   /* write store.pos in fullindex */
			if (res != rnum) {
				flag = false;
				break;
			}
			res = fwrite(length_buf, sizeof(Store_Length), rnum, lenFp);                                             /* write store.len in fullindex */
			if (res != rnum) {
				flag = false;
				break;
			}
			res = fread(srcbuf, sizeof(char), readlen, srcfp);                                                       /* read store.dat.groupNum.0 in index */
			if (res != readlen) {
				flag = false;
				break;
			}
			writelen = readlen;
			res = fwrite(srcbuf, sizeof(char), writelen, destfp);                                                    /* write store.dat.fileNumber in fullindex */
			if (res != writelen) {
				flag = false;
				break;
			}
			readednum += rnum;
			obj_no += rnum;
		}
		if (srcfp != NULL) {
			fclose(srcfp);
			srcfp = NULL;
		}
	}
	if (srcbuf != NULL) free(srcbuf);
	if (posarr != NULL) {
		for (i = 0; i < groupNum; i++) {
			Store_Pos *buffer = posarr[i];
			if (buffer != NULL) {
				free(buffer);
				posarr[i] = NULL;
			}
		}
		free(posarr);
	}
	if (lengtharr != NULL) {
		for (i = 0; i < groupNum; i++) {
			Store_Length *buffer = lengtharr[i];
			if (buffer != NULL) {
				free(buffer);
				lengtharr[i] = NULL;
			}
		}
		free(lengtharr);
	}
	if (destfp != NULL) {
		fclose(destfp);
		destfp = NULL;
	}
	if (posFp != NULL) {
		fclose(posFp);
		posFp = NULL;
	}
	if (lenFp != NULL) {
		fclose(lenFp);
		lenFp = NULL;
	}
	if (pos_buf != NULL) free(pos_buf);
	if (length_buf != NULL) free(length_buf);
	return flag ? 1 : 0;
}

IncrMerger::IncrMerger(void *_conf, char *_outputPath, bool _termDict, 
char *_incrPath, char *_realtimePath, char *_incrLevel, char *_realtimeLevel) 
: Merger(_conf, _outputPath, _termDict, 2, -1) {
	incrPath = _incrPath;
	realtimePath = _realtimePath;
	inputPaths = (char**)malloc(sizeof(char*) * 2);
	inputPaths[0] = incrPath;
	inputPaths[1] = realtimePath;
	incrLevel = _incrLevel;
	realtimeLevel = _realtimeLevel;
	sizeKey = new leveldb::Slice(KEY_SIZE, strlen(KEY_SIZE));
	conjreadBuffer = (char*)malloc(sizeof(char) * 2000);
	conjwriteBuffer = (char*)malloc(sizeof(char) * 2000);
	conjWriteLen = 0;
	conjTotalWrite = 0;
}

IncrMerger::~IncrMerger() {
	int i = 0;
	if (inputPaths != NULL) {
		free(inputPaths);
		inputPaths = NULL;
	}
	if (newDocNums != NULL) {
		free(newDocNums);
		newDocNums = NULL;
	}
	if (newConjIds != NULL) {
		free(newConjIds);
		newConjIds = NULL;
	}
	if (conjreadBuffer != NULL) {
		free(conjreadBuffer);
		conjreadBuffer = NULL;
	}
	if (conjwriteBuffer != NULL) {
		free(conjwriteBuffer);
		conjwriteBuffer = NULL;
	}
	if (newDocIds != NULL) {
		for (i = 0; i < 2; i++) {
			int *newDocId = newDocIds[i];
			if (newDocId != NULL) {
				free(newDocId);
				newDocIds[i] = NULL;
			}
		}
		free(newDocIds);
		newDocIds = NULL;
	}
	if (conjInfo != NULL) {
		for (i = 0; i < 2; i++) {
			MergeConjInfo *info = conjInfo[i];
			if (info != NULL) {
				free(info);
				conjInfo[i] = NULL;
			}
		}
		free(conjInfo);
		conjInfo = NULL;
	}
	delete sizeKey;
}

void IncrMerger::inpath(char *tp, char* name, int group) {
	snprintf(tp, MAX_DIR_LEN, "%s%s", inputPaths[group], name);
}

int IncrMerger::init() {                                         /* arrange new docIds and conjIds */
	int i = 0, j = 0, num = 0, res = 0;
	char tpfile[MAX_DIR_LEN];
	FILE *fp = NULL;
	long filesize = 0;
	newDocNums = (int*)malloc(sizeof(int) * 2);
	bzero(newDocNums, sizeof(int) * 2);
	newDocIds  = (int**)malloc(sizeof(int*) * 2);
	bzero(newDocIds, sizeof(int*) * 2);
	for (i = 0; i < 2; i++) {
		char *inputPath = inputPaths[i];
		if (inputPath == NULL) return 0;
		num	= 0;
		for (j = 0; j < brifeFieldNumber; j++) {                                      /* check brife files */
			IndexField *field = brifeField[j];
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat", inputPath, field->getName());
			fp = fopen(tpfile, "r");
			if (fp == NULL) return 0;
			fseek(fp, 0l, SEEK_END);
			filesize = ftell(fp);
			fclose(fp);
			if (num == 0) num = (int)(filesize / field->getLength());
			else {
				if (num != (int)(filesize / field->getLength())) return 0;
			}
		}
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", inputPath, STORE_POS_FILE_NAME);        /* check store.pos */
		fp = fopen(tpfile, "r");
		if (fp == NULL) return 0;
		fseek(fp, 0l, SEEK_END);
		filesize = ftell(fp);
		fclose(fp);
		if (num != (int)(filesize / 8)) return 0;
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", inputPath, STORE_LEN_FILE_NAME);        /* check store.len */
		fp = fopen(tpfile, "r");
		if (fp == NULL) return 0;
		fseek(fp, 0l, SEEK_END);
		filesize = ftell(fp);
		fclose(fp);
		if (num != (int)(filesize / 4)) return 0;
		docNums[i] = num;
		newDocIds[i] = (int*)malloc(sizeof(int) * num);
		bzero(newDocIds[i], sizeof(int) * num);
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", inputPath, INDEX_DEL);                  /* read del.dat */
		fp = fopen(tpfile, "r");
		if (fp == NULL) return 0;
		int readNum = (num + 7) / 8, n = 0, currentId = 0;
		char *delmap = (char*)malloc(sizeof(char) * readNum);
		res = fread(delmap, sizeof(char), readNum, fp);
		if (res != readNum) return 0;
		fclose(fp);
		for (n = 0; n < num; n++) {                                                  /* arrange new docId into newDocIds, considering deleted docs */
			char temp = delmap[n / 8];
			char delvalue = ((char)1) << (7 - (n % 8));
			if ((temp & delvalue) == delvalue) newDocIds[i][n] = -1;                 /* doc deleted, docId set to -1 */
			else newDocIds[i][n] = currentId++;                                      /* arrange new docId */
		}
		newDocNums[i] = currentId;
		free(delmap);
	}
	if (NULL == incrLevel || NULL == realtimeLevel) return 0;
	conjInfo = (MergeConjInfo**)malloc(sizeof(MergeConjInfo*) * 2);
	bzero(conjInfo, sizeof(MergeConjInfo*) * 2);
	int incrNum = 0, realtimeNum = 0, notfound = 0;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", incrPath, CONJUCTION_INDEX);
	fp = fopen(tpfile, "r");                                                 /* read conj.idx in incr into conjInfo[0] */
	if (fp == NULL) {
		return 0;
	}
	fseek(fp, 0l, SEEK_END);
	filesize = ftell(fp);
	if (filesize == 0 || (filesize % 12) != 0) {
		return 0;
	}
	incrNum = ((int)filesize) / 12;
	incrConj = incrNum;
	conjInfo[0] = (MergeConjInfo*)malloc(12 * incrNum);
	fseek(fp, 0, SEEK_SET);
	res = fread(conjInfo[0], 12, incrNum, fp);
	if (res != incrNum) {
		return 0;
	}
	fclose(fp);
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", realtimePath, CONJUCTION_INDEX);
	fp = fopen(tpfile, "r");                                                 /* read conj.idx in realtime into conjInfo[1] */
	if (fp == NULL) {
		return 0;
	}
	fseek(fp, 0l, SEEK_END);
	filesize = ftell(fp);
	if (filesize == 0 || (filesize % 12) != 0) {
		return 0;
	}
	realtimeNum = ((int)filesize) / 12;
	conjInfo[1] = (MergeConjInfo*)malloc(12 * realtimeNum);
	fseek(fp, 0, SEEK_SET);
	res = fread(conjInfo[1], 12, realtimeNum, fp);
	if (res != realtimeNum) {
		return 0;
	}
	realtimeConj = realtimeNum;
	fclose(fp);
	bool flag = true;
	leveldb::DB *incrdb = NULL, *realtimedb = NULL;
	leveldb::Options options;
	options.create_if_missing = false;
	leveldb::Status status = leveldb::DB::Open(options, incrLevel, &incrdb);
	if (!status.ok()) {
		return 0;
	}
	status = leveldb::DB::Open(options, realtimeLevel, &realtimedb);
	if (!status.ok()) {
		return 0;
	}
	newConjIds = (int*)malloc(sizeof(int) * realtimeNum);
	for (i = 0; i < realtimeNum; i++) {
		newConjIds[i] = 0;
	}
	leveldb::Iterator* it = realtimedb->NewIterator(leveldb::ReadOptions());           /* merge conjunction Id in realtime to incr */
	for (it->SeekToFirst(); flag && it->Valid(); it->Next()) {                         /* iterate all conj in realtime */
		//cout << it->key().ToString() << ": " << it->value().ToString() << endl;
		leveldb::Slice key = it->key();
		string value = it->value().ToString();
		if (*sizeKey == key) continue;
		int conjId = atoi(value.c_str());
		if (conjId >= realtimeNum) flag = false;
		else {
			string strvalue;
			leveldb::Status s = incrdb->Get(leveldb::ReadOptions(), key, &strvalue);
			if (s.ok()) {
				int newconjId = atoi(strvalue.c_str());
				newConjIds[conjId] = 0 - newconjId;                                    /* found in incr, store its conjId */
			} else {
				newConjIds[conjId] = 1;                                                /* not found, mark 1 */
			}
		}
	}
	if (!it->status().ok()) flag = false;
	for (i = 0; i < realtimeNum; i++) {                                                /* arrange conjId for conj not found in incr */
		if (newConjIds[i] > 0) {
			newConjIds[i] = incrNum + (notfound++);                                    /* newly arranged conjIds starts after conjIds in incr */
		}
	}
	delete it;
	if (incrdb != NULL) delete incrdb;
	if (realtimedb != NULL) delete realtimedb;
	return flag ? 1 : 0;
}

/* write conj.idx, conj.dat, max.conjsize, del.dat, update leveldb of incr */
int IncrMerger::over() {
	if (mergeConj() <= 0) return 0;
	int notfound = 0, res = 0;
	leveldb::DB *incrdb = NULL, *realtimedb = NULL;
	leveldb::Options options;
	options.create_if_missing = false;
	leveldb::Status status = leveldb::DB::Open(options, incrLevel, &incrdb);
	if (!status.ok()) return 0;
	status = leveldb::DB::Open(options, realtimeLevel, &realtimedb);
	if (!status.ok()) return 0;
	leveldb::Iterator* it = realtimedb->NewIterator(leveldb::ReadOptions());
	for (it->SeekToFirst(); it->Valid(); it->Next()) {                                       /* update conjId in incr */
		leveldb::Slice key = it->key();
		string value = it->value().ToString();
		if (*sizeKey == key) continue;
		int conjId = atoi(value.c_str());
		if (newConjIds[conjId] > 0) {                                                        /* conj in realtime, not in incr */
		    stringstream sizestr;
			sizestr << newConjIds[conjId];
			string strvalue = sizestr.str();
			leveldb::Status s = incrdb->Put(leveldb::WriteOptions(), key, strvalue);         /* write into incr */
			if (!s.ok()) return 0;
		}		
	}
	if (!it->status().ok()) return 0;
	delete it;
	if (incrdb != NULL) delete incrdb;
	if (realtimedb != NULL) delete realtimedb;
	char tpfile[MAX_DIR_LEN];
	bool flag = true;
	FILE *incrMaxfp = NULL, *realMaxfp = NULL, *maxConjfp = NULL;                            /* write max.conjsize */
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", incrPath, MAX_CONJSIZE);
    incrMaxfp = fopen(tpfile, "r");
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", realtimePath, MAX_CONJSIZE);
    realMaxfp = fopen(tpfile, "r");
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, MAX_CONJSIZE);
    maxConjfp = fopen(tpfile, "w");
	if (incrMaxfp != NULL && realMaxfp != NULL && maxConjfp != NULL) {
		bzero(tpfile, MAX_DIR_LEN);
		res = fread(tpfile, sizeof(char), 10, incrMaxfp);
		if (res < 1) flag = false;
		int incrmaxsize = 0;
		if (flag) incrmaxsize = atoi(tpfile);
		bzero(tpfile, MAX_DIR_LEN);
		res = fread(tpfile, sizeof(char), 10, realMaxfp);
		if (res < 1) flag = false;
		int realmaxsize = 0;
		if (flag) realmaxsize = atoi(tpfile);
		if (incrmaxsize < realmaxsize) incrmaxsize = realmaxsize;
		snprintf(tpfile, MAX_DIR_LEN, "%d\n", incrmaxsize);
		res = fwrite(tpfile, sizeof(char), strlen(tpfile), maxConjfp);
		if (res != (int)strlen(tpfile)) flag = false;
	} else flag = false;
	if (incrMaxfp != NULL) fclose(incrMaxfp);
	if (realMaxfp != NULL) fclose(realMaxfp);
	if (maxConjfp != NULL) fclose(maxConjfp);
	char *deldat = (char*)malloc(sizeof(char) * ((newDocNums[0] + newDocNums[1] + 7) / 8));
	bzero(deldat, sizeof(char) * ((newDocNums[0] + newDocNums[1] + 7) / 8));
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, INDEX_DEL);
	FILE *delfp = fopen(tpfile, "w");
	if (delfp == NULL) flag = false;
	else {
	    res = fwrite(deldat, sizeof(char), (newDocNums[0] + newDocNums[1] + 7) / 8, delfp);     /* write del.dat: all 0 */
		if (res != ((newDocNums[0] + newDocNums[1] + 7) / 8)) flag = false;
		fclose(delfp);
	}
	free(deldat);
	return flag ? 1 : 0;	
}

int IncrMerger::brife() {                       /* merge brife data of un-deleted docs in incr, realtime into merger: first write incr, then realtime */
	char tpfile[MAX_DIR_LEN];
	int i = 0, j = 0, n = 0;
	bool flag = true;
	FILE *brifeInFp = NULL;
	FILE *brifeOutFp = NULL;
	for (i = 0; flag && (i < brifeFieldNumber); i++) {
		IndexField *field = brifeField[i];
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat", outputPath, field->getName());
		brifeOutFp = fopen(tpfile, "w");                                                  /* open brifeName.dat in merger */
		if (brifeOutFp == NULL) {
			flag = false;
			break;
		}
		char *buffer = (char*)malloc(1024 * field->getLength());
		for (j = 0; flag && (j < 2); j++) {                                              /* write brife data of un-deleted docs into merger, first incr, then realtime */
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat", inputPaths[j], field->getName());  /* open brifeName.dat in source dir */
			brifeInFp = fopen(tpfile, "r");
			if (brifeInFp == NULL) {
				flag = false;
				break;
			}
			int begin = 0, end = 0, *ids = NULL;
			ids = newDocIds[j];
			for (n = 0; flag && (n < docNums[j]); n++) {                                 /* iterate all docs in group j */
				if (ids[n] == -1) {                                                      /* skip deleted doc */
					end = n;
					if (end > begin) {
						if (writebrife(brifeInFp, brifeOutFp, field->getLength(), begin, end, buffer) <= 0) {   /* write brife data */
							flag = false;
						}
					}
					begin = n + 1;
				}
			}
			if (end != (docNums[j] - 1)) {
				if (writebrife(brifeInFp, brifeOutFp, field->getLength(), begin, docNums[j], buffer) <= 0) {
					flag = false;
				}
			}
			fclose(brifeInFp);
			brifeInFp = NULL;
		}
		free(buffer);
		fclose(brifeOutFp);
		brifeOutFp = NULL;
	}
	if (brifeInFp != NULL) {
		fclose(brifeInFp);
		brifeInFp = NULL;
	}
	if (brifeOutFp != NULL) {
		fclose(brifeOutFp);
		brifeOutFp = NULL;
	}
	return flag ? 1 : 0;
}

int IncrMerger::writebrife(FILE *brifeInFp, FILE *brifeOutFp, int size, int begin, int end, char* buffer) {
	fseek(brifeInFp, (long)(begin * size), SEEK_SET);
	int res = 0;
	int readNum = end - begin, toRead = 0;
	while (readNum > 0) {
		if (readNum >= 1024) toRead = 1024;
		else toRead = readNum;
		res = fread(buffer, size, toRead, brifeInFp);
		if (res != toRead) return -1;
		res = fwrite(buffer, size, toRead, brifeOutFp);
		if (res != toRead) return -1;
		readNum -= toRead;
	}
	return 1;
}

/* write store.len, store.pos, store.dat in incr, realtime into merger, ignore deleted docs */
int IncrMerger::store() {
	char tpfile[MAX_DIR_LEN];
	int j = 0, res = 0, obj_no = 0;
	bool flag = true;
	FILE **outFp = NULL;
	outFp = (FILE**)malloc(sizeof(FILE*) * 2);
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, STORE_POS_FILE_NAME);
	outFp[0] = fopen(tpfile, "w");                                                             /* open store.pos in merger */
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, STORE_LEN_FILE_NAME);
	outFp[1] = fopen(tpfile, "w");                                                             /* open store.len in merger */
	if (outFp[0] == NULL || outFp[1] == NULL) flag = false;
	posarr = (Store_Pos**)malloc(sizeof(Store_Pos*) * 2);
	lengtharr = (Store_Length**)malloc(sizeof(Store_Length*) * 2);
	srcbuf = (char*)malloc(sizeof(char) * MAX_STORE_UNIT * MAX_STORE_LENGTH);
	if (posarr == NULL || lengtharr == NULL || srcbuf == NULL) flag = false;
	Store_Pos *pos_buf = (Store_Pos*)malloc(MAX_STORE_UNIT * sizeof(Store_Pos));
	Store_Length *length_buf = (Store_Length*)malloc(MAX_STORE_UNIT * sizeof(Store_Length));
	if (pos_buf == NULL || length_buf == NULL) flag = false;
	for (j = 0; (j < 2) && flag; j++) {                                                       /* read store.pos into posarr, read store.len into lengtharr */
		FILE *sublenFp = NULL, *subposFp = NULL;
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", inputPaths[j], STORE_POS_FILE_NAME);
		subposFp = fopen(tpfile, "r");                                                        /* store.pos */
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", inputPaths[j], STORE_LEN_FILE_NAME);
		sublenFp = fopen(tpfile, "r");                                                        /* store.len */
		if (subposFp == NULL || sublenFp == NULL) {
			flag = false;
			break;
		}
		posarr[j] = (Store_Pos*)malloc(sizeof(Store_Pos) * docNums[j]);
		lengtharr[j] = (Store_Length*)malloc(sizeof(Store_Length) * docNums[j]);
		if (lengtharr[j] == NULL || posarr[j] == NULL) flag = false;
		res = fread(posarr[j], sizeof(Store_Pos), docNums[j], subposFp);
		if (res != docNums[j]) {
			flag = false;
			break;
		}
		res = fread(lengtharr[j], sizeof(Store_Length), docNums[j], sublenFp);
		if (res != docNums[j]) {
			flag = false;
			break;
		}
		if (subposFp != NULL) {
			fclose(subposFp);
			subposFp = NULL;
		}
		if (sublenFp != NULL) {
			fclose(sublenFp);
			sublenFp = NULL;
		}
	}
	for (j = 0; flag && (j < 2); j++) {                             /* write store.len, store.pos, store.dat into merger */
		int begin = 0, end = 0, *ids = NULL, n = 0;
		ids = newDocIds[j];
		for (n = 0; flag && (n < docNums[j]); n++) {
			if (ids[n] == -1) {                                     /* skip deleted doc */
				end = n;
				if (end > begin) {
					if (writestore(outFp, pos_buf, length_buf, obj_no, begin, end, j) <= 0) flag = false;
					obj_no += (end - begin);
				}
				begin = n + 1;
			}
		}
		if (end != (docNums[j] - 1)) {
			if (writestore(outFp, pos_buf, length_buf, obj_no, begin, docNums[j], j) <= 0) flag = false;
			obj_no += (docNums[j] - begin);
		}
	}
	if (outFp != NULL) {
		for (j = 0; j < 2; j++) {
			FILE *fp = outFp[j];
			if (fp != NULL) fclose(fp);
			outFp[j] = NULL;
		}
		free(outFp);
	}
	if (posarr != NULL) {
		for (j = 0; j < 2; j++) {
			Store_Pos *pos = posarr[j];
			if (pos != NULL) free(pos);
			posarr[j] = NULL;
		}
		free(posarr);
		posarr = NULL;
	}
	if (lengtharr != NULL) {
		for (j = 0; j < 2; j++) {
			Store_Length *len = lengtharr[j];
			if (len != NULL) free(len);
			lengtharr[j] = NULL;
		}
		free(lengtharr);
		lengtharr = NULL;
	}
	if (srcbuf != NULL) {
		free(srcbuf);
		srcbuf = NULL;
	}
	if (pos_buf != NULL) {
		free(pos_buf);
		pos_buf = NULL;
	}
	if (length_buf != NULL) {
		free(length_buf);
		length_buf = NULL;
	}
	return flag ? 1 : 0;
}

/* write store.pos, store.len, store.dat in inputPaths[group] into merger */
int IncrMerger::writestore(FILE **outFp, Store_Pos *pos_buf, Store_Length *length_buf,
int obj_no, int start, int end, int group) {
	char tpfile[MAX_DIR_LEN];
	int num = 0, readednum = 0, rnum = 0;
	int srcfno = -1, srcobjoff = 0, srcobjleft = 0;
	FILE *srcfp = NULL, *destfp = NULL;
	int curlen = 0, readlen = 0, writelen = 0, obj_id = 0;
	int destfno = -1, destobjoff = 0, destobjleft = 0;
	int ret = 0;
	long curpos = 0;
	bool flag = true;
	num = end - start;
	while (readednum < num) {
		if (num - readednum < MAX_STORE_UNIT) rnum = num - readednum;
		else rnum = MAX_STORE_UNIT;
		int pre = srcfno, preout = destfno;
		srcfno = (start + readednum) / MAX_STORE_DOCNUM;                        /* source: store.dat.srcfno */
		srcobjoff = (start + readednum) % MAX_STORE_DOCNUM;
		srcobjleft = MAX_STORE_DOCNUM - srcobjoff;
		rnum = rnum < srcobjleft ? rnum : srcobjleft;
		destfno = obj_no / MAX_STORE_DOCNUM;                                    /* dest: store.dat.destfno */
		destobjoff = obj_no % MAX_STORE_DOCNUM;
		destobjleft = MAX_STORE_DOCNUM - destobjoff;
		rnum = rnum < destobjleft ? rnum : destobjleft;
		if (pre != srcfno) {                                                    /* pre store.dat.n finished reading, open next one */
			if (srcfp != NULL) fclose(srcfp);
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", inputPaths[group], STORE_FILE_NAME, srcfno);
			srcfp = fopen(tpfile, "r");
			if (srcfp == NULL) {
				flag = false;
				break;
			}
		}
		if (preout != destfno) {                                               /* pre store.dat.n finished writing, open next one */
			if (destfp != NULL) fclose(destfp);
			snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", outputPath, STORE_FILE_NAME, destfno);
			destfp = fopen(tpfile, "a");
			if (destfp == NULL) {
				flag = false;
				break;
			}				
		}
		fseek(srcfp, posarr[group][start + readednum].pos, SEEK_SET);          /* find position to read in store.dat */
		curpos = ftell(destfp);                                                /* find position to write in store.dat */
		readlen = posarr[group][start + readednum + rnum - 1].pos - posarr[group][start + readednum].pos
		+ lengtharr[group][start + readednum + rnum - 1].length;               /* bytes to read in store.dat */
		curlen = 0;
		for (obj_id = 0; obj_id < rnum; obj_id++) {                            /* write store.pos into pos_buf, write store.len into length_buf */
			pos_buf[obj_id].pos = curpos + curlen;
			length_buf[obj_id].length = lengtharr[group][obj_id + start + readednum].length; 
			curlen += lengtharr[group][obj_id + start + readednum].length;
		} 
		if(readlen != curlen) {
			flag = false;
			break;
		}
		ret = fwrite(pos_buf, sizeof(Store_Pos), rnum, outFp[0]);              /* write store.pos */
		if (ret != rnum) {
			flag = false;
			break;
		}
		ret = fwrite(length_buf, sizeof(Store_Length), rnum, outFp[1]);        /* write store.len */
		if (ret != rnum) {
			flag = false;
			break;
		}
		ret = fread(srcbuf, sizeof(char), readlen, srcfp);                     /* read store.dat into srcbuf */
		if (ret != readlen) {
			flag = false;
			break;
		}
		writelen = readlen;
		ret = fwrite(srcbuf, sizeof(char), writelen, destfp);                  /* write store.dat */
		if (ret != writelen) {
			flag = false;
			break;
		}
		readednum += rnum;
		obj_no += rnum;		
	}
	if (srcfp != NULL) fclose(srcfp);
	if (destfp != NULL) fclose(destfp);
	return flag ? 1 : 0;
}

int IncrMerger::writeIndex(int objno, int group, int num) {
	int i = 0, res = 0, writeLen = 0, add = 0;
	assert(objno >= 0);
	for (i = 0; i < num; i++) {
		if (writeLen >= (102400 - 6)) {
			indexWrite += writeLen;
			res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);                 /* write index.dat */
			if (res != writeLen) return -1;
			writeLen = 0;
		}
		int docid = indexReaders[group]->readInt();
		float weight = indexReaders[group]->readUb();
		if (group == 0) {
			docid = newDocIds[0][docid];                                                /* incr */
		} else {
			docid = newDocIds[1][docid];
			if (docid != -1) {
				docid = newDocNums[0] + docid;                                          /* for realtime, docId need to add a shift of doc count of incr */
			}
		}
		if (docid == -1) continue;                                                      /* skip deleted doc */
		add++;
		writeBuffer[writeLen++] = (char)(docid >> 0);                                   /* docId */
		writeBuffer[writeLen++] = (char)(docid >> 8);
		writeBuffer[writeLen++] = (char)(docid >> 16);
		writeBuffer[writeLen++] = (char)(docid >> 24);
		Utils::decode(weight, writeBuffer + writeLen);                                  /* payload */
		writeLen += 2;
	}
	if (writeLen > 0) {
		indexWrite += writeLen;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);                     /* write index.dat */
		if (res != writeLen) return -1;
		writeLen = 0;
	}
	return add;
}

int IncrMerger::writeDnfIndex(int group, int num) {
	int i = 0, res = 0, writeLen = 0, add = 0;
	for (i = 0; i < num; i++) {
		if (writeLen >= (102400 - 6)) {
			indexWrite += writeLen;
			res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);
			if (res != writeLen) return -1;
			writeLen = 0;
		}
		int docid = indexReaders[group]->readInt();
		int belong = indexReaders[group]->read();
		if (group == 1) {
			docid = newConjIds[docid];
			if (docid <= 0) continue;                                           /* conj in realtime and incr, skip this one */
		}
		add++;
		writeBuffer[writeLen++] = (char)(docid >> 0);                           /* new conjId */
		writeBuffer[writeLen++] = (char)(docid >> 8);
		writeBuffer[writeLen++] = (char)(docid >> 16);
		writeBuffer[writeLen++] = (char)(docid >> 24);
		writeBuffer[writeLen++] = (char)belong;                                 /* belong */
	}
	if (writeLen > 0) {
		indexWrite += writeLen;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexFp);
		if (res != writeLen) return -1;
		writeLen = 0;
	}
	return add;
}

int IncrMerger::mergeConj() {                                                             /* merge conj.idx, conj.dat into merger */
	int i = 0, conjId = 0;
	bool flag = true;
	int *samelist = (int*)malloc(sizeof(int) * incrConj);
	if (samelist == NULL) flag = false;
	FILE *incrDat = NULL, *realDat = NULL;
	FILE *index = NULL, *dat = NULL;
	char tpfile[MAX_DIR_LEN];
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", incrPath, CONJUCTION_DAT);
	incrDat = fopen(tpfile, "r");                                                         /* open conj.dat in incr */
	if (incrDat == NULL) flag = false;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", realtimePath, CONJUCTION_DAT);
	realDat = fopen(tpfile, "r");                                                         /* open conj.dat in realtime */
	if (realDat == NULL) flag = false;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, CONJUCTION_INDEX);
	index = fopen(tpfile, "w");                                                           /* open conj.idx in merger */
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", outputPath, CONJUCTION_DAT);
	dat = fopen(tpfile, "w");                                                             /* open conj.dat in merger */
	if (index == NULL || dat == NULL) flag = false;
	char *indexwriteBuffer = (char*)malloc(sizeof(char) * 12);
	int docnum = 0, res = 0;
	for (i = 0; flag && (i < incrConj); i++) {
		samelist[i] = -1;
	}
	for (i = 0; flag && (i < realtimeConj); i++) {
		conjId = newConjIds[i];
		if (conjId <= 0) {
			conjId = 0 - conjId;
			samelist[conjId] = i;                                                          /* samelist[a] = b: a in incr equals b in realtime */
		}
	}
	for (i = 0; flag && (i < incrConj); i++) {                                             /* write conj in incr */
		docnum = 0;
		conjWriteLen = 0;
		indexwriteBuffer[0] = (char)(conjTotalWrite >> 0);                                 /* offset of posting list */
        indexwriteBuffer[1] = (char)(conjTotalWrite >> 8);
        indexwriteBuffer[2] = (char)(conjTotalWrite >> 16);
        indexwriteBuffer[3] = (char)(conjTotalWrite >> 24);
        indexwriteBuffer[4] = (char)(conjTotalWrite >> 32);
        indexwriteBuffer[5] = (char)(conjTotalWrite >> 40);
        indexwriteBuffer[6] = (char)(conjTotalWrite >> 48);
        indexwriteBuffer[7] = (char)(conjTotalWrite >> 56);
		long off = ((MergeConjInfo*)((char*)conjInfo[0] + 12 * i))->offset;                /* offset in conj.dat */
		int length = ((MergeConjInfo*)((char*)conjInfo[0] + 12 * i))->length;              /* length of posting list of this conj */
		fseek(incrDat, off, SEEK_SET);
		res = writeConj(incrDat, dat, length, 0);                                          /* write conj.dat, considering change in docId */
		if (res < 0) {
			flag = false;
			break;
		} else docnum += res;
		int real = samelist[i];
		if (real >= 0) {                                                                   /* same conj in realtime, add posting list */
			off = ((MergeConjInfo*)((char*)conjInfo[1] + 12 * real))->offset;
			length = ((MergeConjInfo*)((char*)conjInfo[1] + 12 * real))->length;
			fseek(realDat, off, SEEK_SET);
			res = writeConj(realDat, dat, length, 1);
			if (res < 0) {
				flag = false;
				break;
			} else docnum += res;
		}
		if (conjWriteLen > 0) {
			res = fwrite(conjwriteBuffer, sizeof(char), conjWriteLen, dat);
			if (res != conjWriteLen) {
				flag = false;
				break;
			}
			conjTotalWrite += conjWriteLen;
		}
		indexwriteBuffer[8] = (char)(docnum >> 0);                                          /* length of posting list */
        indexwriteBuffer[9] = (char)(docnum >> 8);
        indexwriteBuffer[10] = (char)(docnum >> 16);
        indexwriteBuffer[11] = (char)(docnum >> 24);
		res = fwrite(indexwriteBuffer, sizeof(char), 12, index);                            /* write conj.idx */
		if (res != 12) {
			flag = false;
			break;
		}
	}
	for (i = 0; flag && (i < realtimeConj); i++) {                                          /* write conj in realtime */
		conjId = newConjIds[i];
		if (conjId > 0) {                                                                   /* conj not in incr */
			docnum = 0;
			conjWriteLen = 0;
			indexwriteBuffer[0] = (char)(conjTotalWrite >> 0);
			indexwriteBuffer[1] = (char)(conjTotalWrite >> 8);
			indexwriteBuffer[2] = (char)(conjTotalWrite >> 16);
			indexwriteBuffer[3] = (char)(conjTotalWrite >> 24);
			indexwriteBuffer[4] = (char)(conjTotalWrite >> 32);
			indexwriteBuffer[5] = (char)(conjTotalWrite >> 40);
			indexwriteBuffer[6] = (char)(conjTotalWrite >> 48);
			indexwriteBuffer[7] = (char)(conjTotalWrite >> 56);
			long off = ((MergeConjInfo*)((char*)conjInfo[1] + 12 * i))->offset;
			int length = ((MergeConjInfo*)((char*)conjInfo[1] + 12 * i))->length;
			fseek(realDat, off, SEEK_SET);
			res = writeConj(realDat, dat, length, 1);
			if (res < 0) {
				flag = false;
				break;
			} else docnum += res;
			if (conjWriteLen > 0) {
				res = fwrite(conjwriteBuffer, sizeof(char), conjWriteLen, dat);
				if (res != conjWriteLen) {
					flag = false;
					break;
				}
				conjTotalWrite += conjWriteLen;
			}
			indexwriteBuffer[8] = (char)(docnum >> 0);
			indexwriteBuffer[9] = (char)(docnum >> 8);
			indexwriteBuffer[10] = (char)(docnum >> 16);
			indexwriteBuffer[11] = (char)(docnum >> 24);
			res = fwrite(indexwriteBuffer, sizeof(char), 12, index);
			if (res != 12) {
				flag = false;
				break;
			}
		}
	}
	if (samelist != NULL) free(samelist);
	if (index != NULL) fclose(index);
	if (dat != NULL) fclose(dat);
	if (incrDat != NULL) fclose(incrDat);
	if (realDat != NULL) fclose(realDat);
	if (indexwriteBuffer != NULL) free(indexwriteBuffer);
	return flag ? 1 : 0;
}

int IncrMerger::writeConj(FILE *readFp, FILE *writeFp, int length, int group) {
	int toRead = 0, count = 0, docId = 0, docnum = 0;
	int j = 0, res = 0;
	if (length == 0) return res;
	toRead = fread(conjreadBuffer, sizeof(char), 2000, readFp);
	if (toRead <= 0) return -1;
	for (j = 0; j < length; j++) {
		if (count == toRead) {                                                                                 /* buffer finished, fill again */
			toRead = fread(conjreadBuffer, sizeof(char), 2000, readFp);
			if (toRead <= 0) return -1;
			count = 0;
		}
		if (count + 4 <= toRead) {
			docId = (conjreadBuffer[count] & 0xff) + ((conjreadBuffer[count + 1] & 0xff) << 8)                 /* docId */
			+ ((conjreadBuffer[count + 2] & 0xff) << 16) + ((conjreadBuffer[count + 3] & 0xff) << 24);
			count += 4;
		} else {
			int n = toRead - count;
			int m = 4 - n, k = 0;
			for (k = 0; k < n; k++) {
				docId = (conjreadBuffer[count++] & 0xff) << (k * 8);
			}
			toRead = fread(conjreadBuffer, sizeof(char), 2000, readFp);
			if (toRead <= 0) return -1;
			count = 0;
			for (k = 0; k < m; k++) {
				docId = (conjreadBuffer[count++] & 0xff) << ((k + n) * 8);
			}
		}
		if (newDocIds[group][docId] == -1) continue;                                                            /* skip deleted doc */
		docId = newDocIds[group][docId];                                                                        /* get new docId */
		if (group == 1) docId += newDocNums[0];                                                                 /* doc in realtime, add a shift */
		conjwriteBuffer[conjWriteLen++] = (char)(docId >> 0);
		conjwriteBuffer[conjWriteLen++] = (char)(docId >> 8);
		conjwriteBuffer[conjWriteLen++] = (char)(docId >> 16);
		conjwriteBuffer[conjWriteLen++] = (char)(docId >> 24);
		if (conjWriteLen > 1996) {
			res = fwrite(conjwriteBuffer, sizeof(char), conjWriteLen, writeFp);
			if (res != conjWriteLen) {
				return -1;
			}
			conjTotalWrite += conjWriteLen;
			conjWriteLen = 0;
		}
		docnum++;
	}
	return docnum;
}
