#include "inverted.h"

Inverted::Inverted(void *_conf) {
	docid = 0;
	realdocid = 0;
	Conf_t *conf = (Conf_t*)_conf;
	schema = conf->schema;
	int num = schema->size(), i = 0;
	indexFieldNumber = 0;
	indexField = (IndexField**)malloc(sizeof(IndexField*) * num);
	IndexField **fieldArray = schema->toArray();
	analyzers = (Analyzer**)malloc(sizeof(Analyzer*) * num);
	fullDict = conf->fulldict;
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isIndex()) {
			indexField[indexFieldNumber] = get;
			int indexType = get->getType();
			if (indexType == 0) {                                                        /* int */
				analyzers[indexFieldNumber] = new TextAnalyzer(get);
			} else if (indexType == 1) {                                                 /* str */
				analyzers[indexFieldNumber] = new TextAnalyzer(get);
			} else if (indexType == 2) {                                                 /* dnf */
				analyzers[indexFieldNumber] = new DnfAnalyzer(get, fullDict);
			} else if (indexType == 3) {                                                 /* tag */
				analyzers[indexFieldNumber] = new TagsAnalyzer(get);
			} else {                                                                     /* bin */
				exit(-1);
			}
			indexFieldNumber++;
		}
	}
	path = conf->index;
	fileNumber = 0;
	maxdoc = conf->indexmaxdoc;
	infos = (TermInfo**)malloc(sizeof(TermInfo*) * MAX_INDEX_FIELD);
	infoLens = (int*)malloc(sizeof(int) * MAX_INDEX_FIELD);
	infoNum = 0;
	numPostings = 0;
	postingsHashSize = 4;
	postingsHashHalfSize = postingsHashSize / 2;
	postingsHashMask = postingsHashSize - 1;
	postingsHash = (RawPostingList**)malloc(sizeof(RawPostingList*) * postingsHashSize);
	bzero(postingsHash, sizeof(RawPostingList*) * postingsHashSize);
	freePostings = (RawPostingList**)malloc(sizeof(RawPostingList*) * 256);
	bzero(freePostings, sizeof(RawPostingList*) * 256);
	p = NULL;
	freePostingsCount = 0;
	allocator = new Allocator();
	termPool = new ByteBlockPool(allocator);
	termPostPool = new ByteBlockPool(allocator);
	reader = new ByteSliceReader();
	dnfnumPostings = 0;
	dnfpostingsHashSize = 4;
	dnfpostingsHashHalfSize = dnfpostingsHashSize / 2;
	dnfpostingsHashMask = dnfpostingsHashSize - 1;
	dnfpostingsHash = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*) * dnfpostingsHashSize);
	bzero(dnfpostingsHash, sizeof(DnfRawPostingList*) * dnfpostingsHashSize);
	dnffreePostings = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*) * 256);
	bzero(dnffreePostings, sizeof(DnfRawPostingList*) * 256);
	dnfp = NULL;
	dnffreePostingsCount = 0;
	dnftermPool = new ByteBlockPool(allocator);
	dnftermPostPool = new ByteBlockPool(allocator);
	dnfconjPostPool = new ByteBlockPool(allocator);
	dnfconjPostIndex = new IntBlockPool(allocator);
	int conjStart = dnfconjPostPool->newSlice(ByteBlockPool::FIRST_LEVEL_SIZE);                              /* write data of conjId 0 */
	if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
		dnfconjPostIndex->nextBuffer();
	}
	dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
	dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
	TermInfo emptyTerm;                                                                                      /* conjId 0 is for empty term */
	char *temp = (char*)malloc(sizeof(char) * 10);
	bzero(temp, sizeof(char) * 10);
	temp[8] = 1;
	emptyTerm.data = temp;
	emptyTerm.dataLen = 8;
	emptyTerm.payload = temp + 8;
	emptyTerm.payloadLen = 1;
	emptyTerm.field = 0;
	dnftermsNum = 1;
	dnfterms = (TermInfo*)malloc(sizeof(TermInfo));
	dnfterms[0] = emptyTerm;
	conjIds = (int*)malloc(sizeof(int));
	conjIds[0] = 0;
	newIdnum = 0;
	oldIdnum = 0;
	maxconjsize = 0;
	addDnfTerms();
	free(temp);
	free(dnfterms);
	free(conjIds);
}

Inverted::~Inverted() {
	if (indexField != NULL) {
		free(indexField);
		indexField = NULL;
	}
	int i = 0;
	if (analyzers != NULL) {
	    for (i = 0; i < indexFieldNumber; i++) {
		    if (analyzers[i] != NULL) delete analyzers[i];
		    analyzers[i] = NULL;
	    }
	    free(analyzers);
		analyzers = NULL;
	}
	if (termPool != NULL) {
		delete termPool;
		termPool = NULL;
	}
	if (termPostPool != NULL) {
		delete termPostPool;
		termPostPool = NULL;
	}
	if (freePostingsCount > 0) {
		allocator->recyclePostings(freePostings, freePostingsCount);
	}
	if (freePostings != NULL) {
	    free(freePostings);
		freePostings = NULL;
	}
	if (postingsHash != NULL) {
		free(postingsHash);
		postingsHash = NULL;
	}
	if (reader != NULL) {
		delete reader;
		reader = NULL;
	}
	if (dnftermPool != NULL) {
		delete dnftermPool;
		dnftermPool = NULL;
	}
	if (dnftermPostPool != NULL) {
		delete dnftermPostPool;
		dnftermPostPool = NULL;
	}
	if (dnfconjPostPool != NULL) {
		delete dnfconjPostPool;
		dnfconjPostPool = NULL;
	}
	if (dnfconjPostIndex != NULL) {
	    delete dnfconjPostIndex;
		dnfconjPostIndex = NULL;
	}
	if (dnffreePostingsCount > 0) {
		allocator->recycleDnfPostings(dnffreePostings, dnffreePostingsCount);
	}
	if (dnffreePostings != NULL) {
		free(dnffreePostings);
		dnffreePostings = NULL;
	}
	if (dnfpostingsHash != NULL) {
		free(dnfpostingsHash);
		dnfpostingsHash = NULL;
	}
	if (allocator != NULL) {
		delete allocator;
		allocator = NULL;
	}
	if (infos != NULL) {
		free(infos);
		infos = NULL;
	}
	if (infoLens != NULL) {
		free(infoLens);
		infoLens = NULL;
	}
}

int Inverted::close() {
	saveIndex();
	saveConjuction();
	return 1;
}

int Inverted::index(DocParser *parser) {
	if (parser == NULL) return 0;
	begin();
	int i = 0;
	int flag = true;
	for (i = 0; i < indexFieldNumber; i++) {                                  /* extract index field into dnfterms, infos */
		IndexField *field = indexField[i];
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		int indexType = field->getType();
		if ((data == NULL || length <= 0) && indexType != 2) continue;         /* only dnf field is optional */
		if (indexType == 0) {
			if (textanalyze(data, length, i) <= 0) {
				printf("text analyze error for field %s\n", field->getName());
				flag = false;
			}
		} else if (indexType == 1) {
			if (textanalyze(data, length, i) <= 0) {
				printf("text analyze error for field %s\n", field->getName());
				flag = false;
			}
		} else if (indexType == 2) {
			if (dnfanalyze(data, length, i) <= 0) {
				printf("dnf analyze error for field %s\n", field->getName());
				for (int j = 0; j < length; j++) {
					printf("%d ", data[j]);
				}
				printf("\n");
				flag = false;
			}
		} else if (indexType == 3) {
			if (tagsanalyze(data, length, i) <= 0) {
				printf("tags analyze error for field %s\n", field->getName());
				flag = false;
			}
		} else {
			return 0;
		}
	}
	if (!flag) return 0;
	end();
	if (docid == maxdoc) {
		saveIndex();
	}
	return 1;
}

void Inverted::saveIndex() {                                            /* write index to disk */
	if (docid <= 0) return;
	compactPostings();
	dnfcompactPostings();
	sortposting(postingsHash, 0, numPostings - 1);                      /* sort posting in increasing order */
	sortdnfposting(dnfpostingsHash, 0, dnfnumPostings - 1);
	int i = 0, docnum = 0, writeLen = 0, termWriteLen = 0, res = 0;
	int vint = 0;
	long vlong = 0, indexWrite = 0;
	char *writeBuffer = (char*)malloc(sizeof(char) * 6 * maxdoc);
	char *termWriteBuffer = (char*)malloc(sizeof(char) * 200);
	FILE *indexfp, *termfp;
	char tpfile_index[MAX_DIR_LEN];
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, INDEX_INVERT_DAT, fileNumber);
	indexfp = fopen(tpfile_index, "w");
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, INDEX_TERM_DAT, fileNumber);
	termfp = fopen(tpfile_index, "w");
	if (termfp == NULL || indexfp == NULL) exit(0);
	///*
	int termNum = 0;
	long termWrite = 0;
	FILE *termskipfp;
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, INDEX_TERM_SKIP, fileNumber);
	termskipfp = fopen(tpfile_index, "w");
	if (termskipfp == NULL) exit(0);
	//*/
	vint = 1;                                                            /* version */
	termWriteBuffer[0] = (char)(vint >> 0);
	termWriteBuffer[1] = (char)(vint >> 8);
	termWriteBuffer[2] = (char)(vint >> 16);
	termWriteBuffer[3] = (char)(vint >> 24);
	vint = numPostings;                                                  /* terms count */
	termWriteBuffer[4] = (char)(vint >> 0);
	termWriteBuffer[5] = (char)(vint >> 8);
	termWriteBuffer[6] = (char)(vint >> 16);
	termWriteBuffer[7] = (char)(vint >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termfp);              /* write term.dat.fileNumber */
	if (res != 8) exit(0);
	///*
	vint = 1;
	termWriteBuffer[0] = (char)(vint >> 0);
	termWriteBuffer[1] = (char)(vint >> 8);
	termWriteBuffer[2] = (char)(vint >> 16);
	termWriteBuffer[3] = (char)(vint >> 24);
	vint = (numPostings + 127) / 128 + 1;
	termWriteBuffer[4] = (char)(vint >> 0);
	termWriteBuffer[5] = (char)(vint >> 8);
	termWriteBuffer[6] = (char)(vint >> 16);
	termWriteBuffer[7] = (char)(vint >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termskipfp);          /* write term.idx.fileNumber */
	if (res != 8) exit(0);
	//*/
	for (i = 0; i < numPostings; i++) {
		RawPostingList *post = postingsHash[i];
		if (post == NULL) continue;
		docnum = 0;
		writeLen = 0;
		termWriteLen = 0;
		reader->init(termPostPool, post->invertedStart, post->invertedUpto);
		while (!reader->eof()) {
			char b = reader->readByte();
			int shift = 7;
			int readdoc = b & 0x7F;
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);              /* docId */
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			writeBuffer[writeLen++] = reader->readByte();                /* payload */
			writeBuffer[writeLen++] = reader->readByte();
			docnum++;
		}
		vint = post->termLength;                                         /* term length */
		while ((vint & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7F) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		char *text1 = termPool->buffers[post->textStart >> BYTE_BLOCK_SHIFT];
		int pos1 = post->textStart & BYTE_BLOCK_MASK;
		memcpy(termWriteBuffer + termWriteLen, text1 + pos1, post->termLength);   /* term data */
		termWriteLen += post->termLength;
		vint = docnum;                                                            /* doc count */
		while ((vint & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		vlong = indexWrite;                                                       /* shift in index.dat */
		while ((vlong & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f) | 0x80);
			vlong >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f));
	    Utils::decode(post->ub, termWriteBuffer + termWriteLen);                  /* UB */
		termWriteLen += 2;
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termfp);        /* write term.dat.fileNumber */
		if (res != termWriteLen) exit(0);
		///*
		if ((termNum % 128) == 0 || i == (numPostings - 1)) {
			vlong = termWrite;                                                    /* start of term in term.dat */
			int appendLen = termWriteLen;
			while ((vlong & ~0x7F) != 0) {
				termWriteBuffer[appendLen++] = (char)((vlong & 0x7f) | 0x80);
				vlong >>= 7;
			}
			termWriteBuffer[appendLen++] = (char)((vlong & 0x7f));
			res = fwrite(termWriteBuffer, sizeof(char), appendLen, termskipfp);   /* write term.idx.fileNumber */
			if (res != appendLen) exit(0);
		}
		termNum++;
		termWrite += termWriteLen;                                                /* add shift */
		//*/
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexfp);               /* write index.dat.fileNumber */
		if (res != writeLen) exit(0);
		indexWrite += writeLen;
	}
	fclose(indexfp);
	fclose(termfp);
	///*
	fclose(termskipfp);
	//*/
	indexWrite = 0;
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, DNF_INDEX_INVERT_DAT, fileNumber);
	indexfp = fopen(tpfile_index, "w");
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, DNF_INDEX_TERM_DAT, fileNumber);
	termfp = fopen(tpfile_index, "w");
	if (termfp == NULL || indexfp == NULL) exit(0);
	///*
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.%d", path, DNF_INDEX_TERM_SKIP, fileNumber);
	termskipfp = fopen(tpfile_index, "w");
	if (termskipfp == NULL) exit(0);
	termNum = 0;
	//*/
	vint = 1;
	termWriteBuffer[0] = (char)(vint >> 0);
	termWriteBuffer[1] = (char)(vint >> 8);
	termWriteBuffer[2] = (char)(vint >> 16);
	termWriteBuffer[3] = (char)(vint >> 24);
	vint = dnfnumPostings;
	termWriteBuffer[4] = (char)(vint >> 0);
	termWriteBuffer[5] = (char)(vint >> 8);
	termWriteBuffer[6] = (char)(vint >> 16);
	termWriteBuffer[7] = (char)(vint >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termfp);
	if (res != 8) exit(0);
	///*
	vint = 1;
	termWriteBuffer[0] = (char)(vint >> 0);
	termWriteBuffer[1] = (char)(vint >> 8);
	termWriteBuffer[2] = (char)(vint >> 16);
	termWriteBuffer[3] = (char)(vint >> 24);
	vint = (dnfnumPostings + 127) / 128 + 1;
	termWriteBuffer[4] = (char)(vint >> 0);
	termWriteBuffer[5] = (char)(vint >> 8);
	termWriteBuffer[6] = (char)(vint >> 16);
	termWriteBuffer[7] = (char)(vint >> 24);
	res = fwrite(termWriteBuffer, sizeof(char), 8, termskipfp);
	if (res != 8) exit(0);
	//*/
	for (i = 0; i < dnfnumPostings; i++) {
		DnfRawPostingList *post = dnfpostingsHash[i];
		if (post == NULL) continue;
		docnum = 0;
		writeLen = 0;
		termWriteLen = 0;
		reader->init(dnftermPostPool, post->invertedStart, post->invertedUpto);
		/*
		if (i == 0 && isEmpty(post)) {
			writeBuffer[writeLen++] = 0;
			writeBuffer[writeLen++] = 0;
			writeBuffer[writeLen++] = 0;
			writeBuffer[writeLen++] = 0;
			writeBuffer[writeLen++] = 1;
			docnum++;
			emptyTerm = true;
		}
		*/
		while (!reader->eof()) {
			char b = reader->readByte();
			int shift = 7;
			int readdoc = b & 0x7F;
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);                       /* conjId */
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			writeBuffer[writeLen++] = reader->readByte();                         /* belong */
			docnum++;
		}
		char *text1 = dnftermPool->buffers[post->textStart >> BYTE_BLOCK_SHIFT];
		int pos1 = post->textStart & BYTE_BLOCK_MASK;
		memcpy(termWriteBuffer + termWriteLen, text1 + pos1, 9);                  /* dnfTerm data */
		termWriteLen += 9;
		vint = docnum;                                                            /* doc count */
		termWriteBuffer[termWriteLen++] = (char)(vint >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 24);
		vlong = indexWrite;                                                       /* shift in dnfindex.dat, 8byte */
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 24);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 32);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 40);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 48);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 56);
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termfp);          /* write dnfterm.dat */
		if (res != termWriteLen) exit(0);
		///*
		if ((termNum % 128) == 0 || i == (dnfnumPostings - 1)) {
			res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termskipfp);  /* write dnfterm.idx */
			if (res != termWriteLen) exit(0);
		}
		termNum++;
		//*/
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexfp);                 /* write dnfindex.dat */
		if (res != writeLen) exit(0);
		indexWrite += writeLen;
	}
	fclose(indexfp);
	fclose(termfp);
	///*
	fclose(termskipfp);
	//*/
	free(writeBuffer);
	free(termWriteBuffer);
	fileNumber++;
	termPool->reset();                                                              /* clear pools, postings */
	termPostPool->reset();
	allocator->recyclePostings(postingsHash, numPostings);
	p = NULL;
	numPostings = 0;
	postingsHashSize = 4;
	postingsHashHalfSize = postingsHashSize / 2;
	postingsHashMask = postingsHashSize - 1;
	dnftermPool->reset();
	dnftermPostPool->reset();
	allocator->recycleDnfPostings(dnfpostingsHash, dnfnumPostings);
	dnfp = NULL;
	dnfnumPostings = 0;
	dnfpostingsHashSize = 4;
	dnfpostingsHashHalfSize = dnfpostingsHashSize / 2;
	dnfpostingsHashMask = dnfpostingsHashSize - 1;
	bzero(postingsHash, sizeof(RawPostingList*) * postingsHashSize);
	bzero(freePostings, sizeof(RawPostingList*) * 256);
	bzero(dnfpostingsHash, sizeof(DnfRawPostingList*) * dnfpostingsHashSize);
	bzero(dnffreePostings, sizeof(DnfRawPostingList*) * 256);
	freePostingsCount = 0;
	dnffreePostingsCount = 0;
	docid = 0;                                                                       /* reset docid */
	TermInfo emptyTerm;
	char *temp = (char*)malloc(sizeof(char) * 10);
	bzero(temp, sizeof(char) * 10);
	temp[8] = 1;
	emptyTerm.data = temp;
	emptyTerm.dataLen = 8;
	emptyTerm.payload = temp + 8;
	emptyTerm.payloadLen = 1;
	emptyTerm.field = 0;
	dnftermsNum = 1;
	dnfterms = (TermInfo*)malloc(sizeof(TermInfo));
	dnfterms[0] = emptyTerm;
	conjIds = (int*)malloc(sizeof(int));
	conjIds[0] = 0;
	newIdnum = 0;
	oldIdnum = 0;
	addDnfTerms();
	free(temp);
	free(dnfterms);
	free(conjIds);
}

bool Inverted::isEmpty(DnfRawPostingList *post) {
	char *text = dnftermPool->buffers[post->textStart >> BYTE_BLOCK_SHIFT];
	int pos = post->textStart & BYTE_BLOCK_MASK, i = 0;
	for (i = 0; i < 9; i++) {
		if (text[pos + i] != 0) {
			return false;
		}
	}
	return true;
}

void Inverted::saveConjuction() {
	FILE *indexfp, *conjfp;
	char tpfile[MAX_DIR_LEN];
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, CONJUCTION_INDEX);
	indexfp = fopen(tpfile, "w");
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, CONJUCTION_DAT);
	conjfp = fopen(tpfile, "w");
	char *writeBuffer = (char*)malloc(sizeof(char) * 2000);
	char *indexwriteBuffer = (char*)malloc(sizeof(char) * 12);
	if (conjfp == NULL || indexfp == NULL) exit(0);
	int i = 0, docnum = 0, writeLen = 0, res = 0;
	long totalWrite = 0;
	for (i = 0; i < dnfconjPostIndex->intOffset + dnfconjPostIndex->intUpto; i = i + 2) {
		int index = i;
		int *data = dnfconjPostIndex->buffers[index >> INT_BLOCK_SHIFT];
		int offset = index & INT_BLOCK_MASK;
		reader->init(dnfconjPostPool, data[offset], data[offset + 1]);
		docnum = 0;
		writeLen = 0;
		indexwriteBuffer[0] = (char)(totalWrite >> 0);                                  /* shift in conj.dat, 8byte */
        indexwriteBuffer[1] = (char)(totalWrite >> 8);
        indexwriteBuffer[2] = (char)(totalWrite >> 16);
        indexwriteBuffer[3] = (char)(totalWrite >> 24);
        indexwriteBuffer[4] = (char)(totalWrite >> 32);
        indexwriteBuffer[5] = (char)(totalWrite >> 40);
        indexwriteBuffer[6] = (char)(totalWrite >> 48);
        indexwriteBuffer[7] = (char)(totalWrite >> 56);
		while (!reader->eof()) {
			char b = reader->readByte();
			int shift = 7;
			int readdoc = b & 0x7F;
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);                             /* docId */
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			if (writeLen > 1996) {
				res = fwrite(writeBuffer, sizeof(char), writeLen, conjfp);              /* write conj.dat */
				if (res != writeLen) exit(0);
				totalWrite += writeLen;
				writeLen = 0;
			}
			docnum++;
		}
		if (writeLen > 0) {
			res = fwrite(writeBuffer, sizeof(char), writeLen, conjfp);
			if (res != writeLen) exit(0);
			totalWrite += writeLen;
		}
		indexwriteBuffer[8] = (char)(docnum >> 0);                                      /* doc count, 4 byte */
        indexwriteBuffer[9] = (char)(docnum >> 8);
        indexwriteBuffer[10] = (char)(docnum >> 16);
        indexwriteBuffer[11] = (char)(docnum >> 24);
		res = fwrite(indexwriteBuffer, sizeof(char), 12, indexfp);                      /* write conj.idx */
		if (res != 12) exit(0);
	}
	fclose(indexfp);
	fclose(conjfp);
	free(writeBuffer);
	free(indexwriteBuffer);
	dnfconjPostPool->reset();
	dnfconjPostIndex->reset();
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", path, MAX_CONJSIZE);
	FILE *maxconjfp = NULL;
	maxconjfp = fopen(tpfile, "w");
	if (maxconjfp != NULL) {
		snprintf(tpfile, MAX_DIR_LEN, "%d\n", maxconjsize);                              /* write max.conjsize */
		res = fwrite(tpfile, sizeof(char), strlen(tpfile), maxconjfp);
		if (res != (int)strlen(tpfile)) exit(0);
		fclose(maxconjfp);
	} else exit(0);
}

void Inverted::sortposting(RawPostingList **postings, int lo, int hi) {
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (RawPostingListCmp(postings[lo], postings[hi]) > 0) {
			RawPostingList *tmp = postings[lo];
			postings[lo] = postings[hi];
			postings[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (RawPostingListCmp(postings[lo], postings[mid]) > 0) {
		RawPostingList *tmp = postings[lo];
		postings[lo] = postings[mid];
		postings[mid] = tmp;
	}
	if (RawPostingListCmp(postings[mid], postings[hi]) > 0) {
		RawPostingList *tmp = postings[mid];
		postings[mid] = postings[hi];
		postings[hi] = tmp;
		if (RawPostingListCmp(postings[lo], postings[mid]) > 0) {
			RawPostingList *tmp2 = postings[lo];
			postings[lo] = postings[mid];
			postings[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	RawPostingList *partition = postings[mid];
	for (;;) {
		while (RawPostingListCmp(postings[right], partition) > 0) --right;
		while (left < right && RawPostingListCmp(postings[left], partition) <= 0) ++left;
		if (left < right) {
			RawPostingList *tmp = postings[left];
			postings[left] = postings[right];
			postings[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	sortposting(postings, lo, left);
	sortposting(postings, left + 1, hi);
}

void Inverted::sortdnfposting(DnfRawPostingList **postings, int lo, int hi) {
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (DnfRawPostingListCmp(postings[lo], postings[hi]) > 0) {
			DnfRawPostingList *tmp = postings[lo];
			postings[lo] = postings[hi];
			postings[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (DnfRawPostingListCmp(postings[lo], postings[mid]) > 0) {
		DnfRawPostingList *tmp = postings[lo];
		postings[lo] = postings[mid];
		postings[mid] = tmp;
	}
	if (DnfRawPostingListCmp(postings[mid], postings[hi]) > 0) {
		DnfRawPostingList *tmp = postings[mid];
		postings[mid] = postings[hi];
		postings[hi] = tmp;
		if (DnfRawPostingListCmp(postings[lo], postings[mid]) > 0) {
			DnfRawPostingList *tmp2 = postings[lo];
			postings[lo] = postings[mid];
			postings[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	DnfRawPostingList *partition = postings[mid];
	for (;;) {
		while (DnfRawPostingListCmp(postings[right], partition) > 0) --right;
		while (left < right && DnfRawPostingListCmp(postings[left], partition) <= 0) ++left;
		if (left < right) {
			DnfRawPostingList *tmp = postings[left];
			postings[left] = postings[right];
			postings[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	sortdnfposting(postings, lo, left);
	sortdnfposting(postings, left + 1, hi);
}

int Inverted::RawPostingListCmp(RawPostingList *p1, RawPostingList *p2) {
	if (p1 == p2) return 0;
	char *text1 = termPool->buffers[p1->textStart >> BYTE_BLOCK_SHIFT];
	int pos1 = p1->textStart & BYTE_BLOCK_MASK;
	char *text2 = termPool->buffers[p2->textStart >> BYTE_BLOCK_SHIFT];
	int pos2 = p2->textStart & BYTE_BLOCK_MASK;
	if (p1->termLength > p2->termLength) {
		int res = memcmp(text2 + pos2, text1 + pos1, p2->termLength);
		if (res > 0) return -1;
		else if (res == 0) return 1;
		else return 1;
	} else if (p1->termLength == p2->termLength) {
		return memcmp(text1 + pos1, text2 + pos2, p1->termLength);
	} else {
		int res = memcmp(text1 + pos1, text2 + pos2, p1->termLength);
		if (res > 0) return 1;
		else if (res == 0) return -1;
		else return -1; 
	}
}

int Inverted::DnfRawPostingListCmp(DnfRawPostingList *p1, DnfRawPostingList *p2) {
	if (p1 == p2) return 0;
	char *text1 = dnftermPool->buffers[p1->textStart >> BYTE_BLOCK_SHIFT];
	int pos1 = p1->textStart & BYTE_BLOCK_MASK;
	char *text2 = dnftermPool->buffers[p2->textStart >> BYTE_BLOCK_SHIFT];
	int pos2 = p2->textStart & BYTE_BLOCK_MASK;
	return memcmp(text1 + pos1, text2 + pos2, 9);
}

void Inverted::compactPostings() {
	int upto = 0, i = 0;
	for (i = 0; i < postingsHashSize; i++) {
		if (postingsHash[i] != NULL) {
			if (upto < i) {
				postingsHash[upto] = postingsHash[i];
				postingsHash[i] = NULL;
			}
			upto++;
		}
	}
	assert(upto == numPostings);
}

void Inverted::dnfcompactPostings() {
	int upto = 0, i = 0;
	for (i = 0; i < dnfpostingsHashSize; i++) {
		if (dnfpostingsHash[i] != NULL) {
			if (upto < i) {
				dnfpostingsHash[upto] = dnfpostingsHash[i];
				dnfpostingsHash[i] = NULL;
			}
			upto++;
		}
	}
	assert(upto == dnfnumPostings);
}

int Inverted::begin() {
	infoNum = 0;
	dnfterms = NULL;
	conjIds = NULL;
	newIdnum = 0;
	oldIdnum = 0;
	newIds = NULL;
	oldIds = NULL;
	dnftermsNum = 0;
	return 1;
}

int Inverted::textanalyze(char *data, int length, int fieldId) {
	Analyzer *textAnalyzer = analyzers[fieldId];
	textAnalyzer->init(data, length);
	int res = textAnalyzer->analyze();
	if (res <= 0) return 0;
	TermInfo* terms = textAnalyzer->getTerms();
	int termsNum = textAnalyzer->getTermNum();
	if (terms == NULL || termsNum != 1) {
		return 0;
	} else {
		infos[infoNum] = terms;
		infoLens[infoNum] = termsNum;
		infoNum++;
	}
	return 1;
}

int Inverted::tagsanalyze(char *data, int length, int fieldId) {
	Analyzer *tagAnalyzer = analyzers[fieldId];
	tagAnalyzer->init(data, length);
	int res = tagAnalyzer->analyze();
	if (res <= 0) return 0;
	TermInfo* terms = tagAnalyzer->getTerms();
	int termsNum = tagAnalyzer->getTermNum();
	if (terms == NULL || termsNum <= 0) {
		return 0;
	} else {
		infos[infoNum] = terms;
		infoLens[infoNum] = termsNum;
		infoNum++;
	}
	return 1;
}

int Inverted::dnfanalyze(char *data, int length, int fieldId) {
	DnfAnalyzer *dnfAnalyzer = (DnfAnalyzer*)analyzers[fieldId];
	dnfAnalyzer->init(data, length);
	int res = dnfAnalyzer->analyze();
	if (res <= 0) return 0;
	dnfterms = dnfAnalyzer->getTerms();
	dnftermsNum = dnfAnalyzer->getTermNum();
	conjIds = dnfAnalyzer->termConjuction();
	newIdnum = dnfAnalyzer->getNewConjuctionNum();
	oldIdnum = dnfAnalyzer->getOldConjuctionNum();
	newIds = dnfAnalyzer->getNewConjuction();
	oldIds = dnfAnalyzer->getOldConjuction();
	if (dnftermsNum > 0 && newIdnum <= 0) return 0;
	if (newIdnum <= 0 && oldIdnum <= 0 && dnftermsNum <= 0) return 0;
	if (newIdnum > 0) {
		int i = 0, base = dnfconjPostIndex->intOffset +dnfconjPostIndex->intUpto;
		for (i = 0; i < newIdnum; i++) {
			if ((newIds[i] * 2) != (base + i * 2)) {                              /* each conj takes 2 int */
				return 0;
			}
		}
	}
	int maxsize = dnfAnalyzer->getMaxConjSize();
	if (maxsize > maxconjsize) maxconjsize = maxsize;
	return 1;
}

int Inverted::end() {
	addTerms();
	addDnfTerms();
	docid++;
	realdocid++;
	return 1;
}

void Inverted::addTerms() {
	if (infoNum <= 0) return;
	int i = 0, j = 0;
	for (i = 0; i < infoNum; i++) {
		TermInfo *info = infos[i];
		int termLen = infoLens[i];
		for (j = 0; j < termLen; j++) {
			TermInfo terminfo = info[j];
			char *data = terminfo.data;
			int dataLen = terminfo.dataLen;
			char *payload = terminfo.payload;
			int payloadLen = terminfo.payloadLen;
			char field = terminfo.field;
			int code = Utils::termHash(terminfo);
			int hashPos = code & postingsHashMask;
			p = postingsHash[hashPos];
			if (p != NULL && !postingEquals(field, data, dataLen)) {
				int inc = ((code >> 8) + code) | 1;
				do {
					code += inc;
					hashPos = code & postingsHashMask;
					p = postingsHash[hashPos];
				} while (p != NULL && !postingEquals(field, data, dataLen));
			}
			if (p == NULL) {
				int textLen1 = 1 + dataLen;
				if (textLen1 + termPool->byteUpto > BYTE_BLOCK_SIZE) {
					termPool->nextBuffer();
				}
				if (0 == freePostingsCount) morePostings();
				p = freePostings[--freePostingsCount];
				assert(p != NULL);
				char *text = termPool->buffer;
				int textUpto = termPool->byteUpto;
				p->textStart = textUpto + termPool->byteOffset;
				p->termLength = (short)textLen1;
				termPool->byteUpto += textLen1;
				text[textUpto] = field;
				memcpy(text + textUpto + 1, data, dataLen);
				assert(postingsHash[hashPos] == NULL);
				postingsHash[hashPos] = p;
				numPostings++;
				if (numPostings == postingsHashHalfSize) {
					rehashPostings(2 * postingsHashSize);
				}
				if (BYTE_BLOCK_SIZE - termPostPool->byteUpto < ByteBlockPool::FIRST_LEVEL_SIZE) {
					termPostPool->nextBuffer();
				}
				int upto = termPostPool->newSlice(ByteBlockPool::FIRST_LEVEL_SIZE);
				p->invertedStart = upto + termPostPool->byteOffset;
				p->invertedUpto = p->invertedStart;
				termVInt(docid);
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					termByte(payload[payi]);
				}
				float weight = Utils::encode(payload);
				p->ub = weight;
			} else {
				termVInt(docid);
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					termByte(payload[payi]);
				}
				float weight = Utils::encode(payload);
				if (weight > p->ub) p->ub = weight;                          /* store max ub */
			}
		}
	}
}

void Inverted::addDnfTerms() {
	int i = 0, j = 0;
	if (dnftermsNum > 0) {
		for (j = 0; j < dnftermsNum; j++) {
			TermInfo terminfo = dnfterms[j];
			char *data = terminfo.data;
			int dataLen = terminfo.dataLen;
			char *payload = terminfo.payload;
			int payloadLen = terminfo.payloadLen;
			char field = terminfo.field;
			int code = Utils::termHash(terminfo);
			int hashPos = code & dnfpostingsHashMask;
			dnfp = dnfpostingsHash[hashPos];
			if (dnfp != NULL && !dnfpostingEquals(field, data, dataLen)) {
				int inc = ((code >> 8) + code) | 1;
				do {
					code += inc;
					hashPos = code & dnfpostingsHashMask;
					dnfp = dnfpostingsHash[hashPos];
				} while (dnfp != NULL && !dnfpostingEquals(field, data, dataLen));
			}
			if (dnfp == NULL) {                                              /* new term */
				int textLen1 = 1 + dataLen;
				if (textLen1 + dnftermPool->byteUpto > BYTE_BLOCK_SIZE) {
					dnftermPool->nextBuffer();
				}
				if (0 == dnffreePostingsCount) dnfmorePostings();
				dnfp = dnffreePostings[--dnffreePostingsCount];
				assert(dnfp != NULL);
				char *text = dnftermPool->buffer;
				int textUpto = dnftermPool->byteUpto;
				dnfp->textStart = textUpto + dnftermPool->byteOffset;
				dnftermPool->byteUpto += textLen1;
				text[textUpto] = field;
				memcpy(text + textUpto + 1, data, dataLen);                  /* write TermInfo.field(1byte),data(8byte) in dnftermPool */
				assert(dnfpostingsHash[hashPos] == NULL);
				dnfpostingsHash[hashPos] = dnfp;
				dnfnumPostings++;
				if (dnfnumPostings == dnfpostingsHashHalfSize) {
					dnfrehashPostings(2 * dnfpostingsHashSize);
				}
				if (BYTE_BLOCK_SIZE - dnftermPostPool->byteUpto < ByteBlockPool::FIRST_LEVEL_SIZE) {
					dnftermPostPool->nextBuffer();
				}
				int upto = dnftermPostPool->newSlice(ByteBlockPool::FIRST_LEVEL_SIZE);
				dnfp->invertedStart = upto + dnftermPostPool->byteOffset;
				dnfp->invertedUpto = dnfp->invertedStart;
				dnftermVInt(conjIds[j]);                                    /* write conjId,payload(1byte) in dnftermPostPool*/
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					dnftermByte(payload[payi]);
				}
			} else {                                                        /* old term, add conjId */
				dnftermVInt(conjIds[j]);
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					dnftermByte(payload[payi]);
				}
			}
		}
	}
	if (newIdnum > 0) {
		for (i = 0; i < newIdnum; i++) {
			int conjStart = dnfconjPostPool->newSlice(ByteBlockPool::FIRST_LEVEL_SIZE);         /* alloc slice for new conjunction */
			if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
				dnfconjPostIndex->nextBuffer();
			}
			dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
			if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
				dnfconjPostIndex->nextBuffer();
			}
			dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
			conjVInt(dnfconjPostIndex->intOffset + dnfconjPostIndex->intUpto - 1, realdocid);    /* write docId into dnfconjPostPool */
		}
	}
	if (oldIdnum > 0) {                                                                          /* add docId for old conjunction */
		for (i = 0; i < oldIdnum; i++) {
			int currentConId = oldIds[i];
			int index = currentConId * 2 + 1;
			conjVInt(index, realdocid);
		}
	}
}

void Inverted::conjVInt(int off, int i) {
	while ((i & ~0x7F) != 0) {
		conjByte(off, (char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	return conjByte(off, (char)i);
}

void Inverted::conjByte(int off, char b) {
	int *data = dnfconjPostIndex->buffers[off >> INT_BLOCK_SHIFT];
	int datoff = off & INT_BLOCK_MASK;
	int upto = data[datoff];
	char *bytes = dnfconjPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	//assert(bytes != NULL);
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {
		offset = dnfconjPostPool->allocSlice(bytes, offset);
		bytes = dnfconjPostPool->buffer;
		data[datoff] = offset + 1 + dnfconjPostPool->byteOffset;                                  /* update dnfconjPostIndex */
	} else {
		data[datoff] = upto + 1;
	}
	bytes[offset] = b;                                                                            /* write byte */
}

bool Inverted::postingEquals(char field, char* tokenText, int tokenTextLen) {
	char* text = termPool->buffers[p->textStart >> BYTE_BLOCK_SHIFT];
	assert(text != NULL);
	int pos = p->textStart & BYTE_BLOCK_MASK;
	if (tokenTextLen + 1 != p->termLength) return false;
	if (field != text[pos++]) return false;
	int tokenPos = 0;
	for (; tokenPos < tokenTextLen; pos++, tokenPos++) {
		if (tokenText[tokenPos] != text[pos]) return false;
	}
	return true;
}

bool Inverted::dnfpostingEquals(char field, char* tokenText, int tokenTextLen) {
	char* text = dnftermPool->buffers[dnfp->textStart >> BYTE_BLOCK_SHIFT];
	assert(text != NULL);
	int pos = dnfp->textStart & BYTE_BLOCK_MASK;
	if (tokenTextLen + 1 != 9) return false;
	if (field != text[pos++]) return false;
	int tokenPos = 0;
	for (; tokenPos < tokenTextLen; pos++, tokenPos++) {
		if (tokenText[tokenPos] != text[pos]) return false;
	}
	return true;
}

void Inverted::morePostings() {
	assert(freePostingsCount == 0);
	allocator->getPostings(freePostings, 256);
	freePostingsCount = 256;
	assert(noNullPostings(freePostings, freePostingsCount));
}

void Inverted::dnfmorePostings() {
	assert(dnffreePostingsCount == 0);
	allocator->getDnfPostings(dnffreePostings, 256);
	dnffreePostingsCount = 256;
	assert(dnfnoNullPostings(dnffreePostings, dnffreePostingsCount));
}

bool Inverted::noNullPostings(RawPostingList** postings, int count) {
	int i = 0;
	for (i = 0; i < count; i++) {
		assert(postings[i] != NULL);
		bzero(postings[i], sizeof(RawPostingList));
	}
	return true;
}

bool Inverted::dnfnoNullPostings(DnfRawPostingList** postings, int count) {
	int i = 0;
	for (i = 0; i < count; i++) {
		assert(postings[i] != NULL);
		bzero(postings[i], sizeof(DnfRawPostingList));
	}
	return true;
}

void Inverted::termVInt(int i) {
	while ((i & ~0x7F) != 0) {
		termByte((char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	termByte((char)i);
}

void Inverted::dnftermVInt(int i) {
	while ((i & ~0x7F) != 0) {
		dnftermByte((char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	dnftermByte((char)i);
}

void Inverted::termByte(char b) {
	int upto = p->invertedUpto;
	char *bytes = termPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	//assert(bytes != NULL);
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {
		offset = termPostPool->allocSlice(bytes, offset);
		bytes = termPostPool->buffer;
		p->invertedUpto = offset + 1 + termPostPool->byteOffset;
	} else {
		p->invertedUpto = p->invertedUpto + 1;
	}
	bytes[offset] = b;
}

/* write a char into dnftermPostPool*/
void Inverted::dnftermByte(char b) {
	int upto = dnfp->invertedUpto;
	char *bytes = dnftermPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	//assert(bytes != NULL);
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {                             /* reach the end of a slice, ie: last byte of slice in first level is 16 */
		offset = dnftermPostPool->allocSlice(bytes, offset);
		bytes = dnftermPostPool->buffer;
		dnfp->invertedUpto = offset + 1 + dnftermPostPool->byteOffset;
	} else {
		dnfp->invertedUpto = dnfp->invertedUpto  + 1;
	}
	bytes[offset] = b;
}

void Inverted::rehashPostings(int newSize) {
	int newMask = newSize - 1, i = 0, j = 0;
	RawPostingList **newHash = (RawPostingList**)malloc(sizeof(RawPostingList*) * newSize);
	bzero(newHash, sizeof(RawPostingList*) * newSize);
	for (i = 0; i < postingsHashSize; i++) {
		RawPostingList *p0 = postingsHash[i];
		if (p0 != NULL) {
			int code = 0;
			int start = p0->textStart & BYTE_BLOCK_MASK;
			short textLen = p0->termLength;
			char *text = termPool->buffers[p0->textStart >> BYTE_BLOCK_SHIFT];
			for (j = textLen; j > 0; j--) {
				code = (code * 31) + text[start + j - 1];
			}
			int hashPos = code & newMask;
			assert(hashPos >= 0);
			if (newHash[hashPos] != NULL) {
				int inc = ((code >> 8) + code) | 1;
				do {
					code += inc;
					hashPos = code & newMask;
				} while (newHash[hashPos] != NULL);
			}
			newHash[hashPos] = p0;
		}
	}
	RawPostingList **oldlist = postingsHash;
	postingsHashMask = newMask;
	postingsHash = newHash;
	postingsHashSize = newSize;
	postingsHashHalfSize = newSize >> 1;
	free(oldlist);
}

void Inverted::dnfrehashPostings(int newSize) {
	int newMask = newSize - 1, i = 0, j = 0;
	DnfRawPostingList **newHash = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*) * newSize);
	bzero(newHash, sizeof(DnfRawPostingList*) * newSize);
	for (i = 0; i < dnfpostingsHashSize; i++) {
		DnfRawPostingList *p0 = dnfpostingsHash[i];
		if (p0 != NULL) {
			int code = 0;
			int start = p0->textStart & BYTE_BLOCK_MASK;
			short textLen = 9;
			char *text = dnftermPool->buffers[p0->textStart >> BYTE_BLOCK_SHIFT];
			for (j = textLen; j > 0; j--) {
				code = (code * 31) + text[start + j - 1];
			}
			int hashPos = code & newMask;
			assert(hashPos >= 0);
			if (newHash[hashPos] != NULL) {
				int inc = ((code >> 8) + code) | 1;
				do {
					code += inc;
					hashPos = code & newMask;
				} while (newHash[hashPos] != NULL);
			}
			newHash[hashPos] = p0;
		}
	}
	DnfRawPostingList **oldlist = dnfpostingsHash;
	dnfpostingsHashMask = newMask;
	dnfpostingsHash = newHash;
	dnfpostingsHashSize = newSize;
	dnfpostingsHashHalfSize = newSize >> 1;
	free(oldlist);
}
