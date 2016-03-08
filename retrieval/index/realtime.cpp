#include "realtime.h"

TermDict::TermDict(ByteBlockPool *_termPool, ByteBlockPool *_dnftermPool, SimpleGc *_gc) {
	termPool = _termPool;
	count = 0;
	termHash = (TermHash*)malloc(sizeof(TermHash));
	int initSize = 4;
	termHash->hashSize = initSize;
	termHash->hashHalfSize = initSize >> 1;
	termHash->hashMask = initSize - 1;
	termHash->ords = (int*)malloc(sizeof(int) * initSize);
	int i = 0;
	for (i = 0; i < initSize; i++) {
		termHash->ords[i] = -1;
	}
	dnftermPool = _dnftermPool;
	dnfcount = 0;
	dnftermHash = (TermHash*)malloc(sizeof(TermHash));
	dnftermHash->hashSize = initSize;
	dnftermHash->hashHalfSize = initSize >> 1;
	dnftermHash->hashMask = initSize - 1;
	dnftermHash->ords = (int*)malloc(sizeof(int) * initSize);
	for (i = 0; i < initSize; i++) {
		dnftermHash->ords[i] = -1;
	}
	positions = (RawPostingList**)malloc(sizeof(RawPostingList*));
	positions[0] = (RawPostingList*)malloc(sizeof(RawPostingList) * 10240);
	bzero(positions[0], sizeof(RawPostingList) * 10240);
	for (i = 0; i < 10240; i++) {
		positions[0][i].invertedStart = -1;
	}
	positionNum = 1;
	dnfpositions = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*));
	dnfpositions[0] = (DnfRawPostingList*)malloc(sizeof(DnfRawPostingList) * 10240);
	bzero(dnfpositions[0], sizeof(DnfRawPostingList) * 10240);
	for (i = 0; i < 10240; i++) {
		dnfpositions[0][i].invertedStart = -1;
	}
	dnfpositionNum = 1;
	gc = _gc;
}

TermDict::~TermDict() {
	int i = 0;
	if (termHash != NULL) {
		if (termHash->ords != NULL) free(termHash->ords);
		free(termHash);
		termHash = NULL;
	}
	if (dnftermHash != NULL) {
		if (dnftermHash->ords != NULL) free(dnftermHash->ords);
		free(dnftermHash);
		dnftermHash = NULL;
	}
	if (positions != NULL) {
		for (i = 0; i < positionNum; i++) {
			RawPostingList *buffer = positions[i];
			if (buffer != NULL) free(buffer);
			positions[i] = NULL;
		}
		free(positions);
		positions = NULL;
	}
	if (dnfpositions != NULL) {
		for (i = 0; i < dnfpositionNum; i++) {
			DnfRawPostingList *buffer = dnfpositions[i];
			if (buffer != NULL) free(buffer);
			dnfpositions[i] = NULL;
		}
		free(dnfpositions);
		dnfpositions = NULL;
	}
}

int TermDict::addTerm(TermInfo terminfo) {                              /* new term: add term to termPool, positions, termHash, return termId; old term: return termId */
	char *data = terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = termHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];
	if (e != -1 && !postingEquals(e, field, data, dataLen)) {
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !postingEquals(e, field, data, dataLen));
	}
	if (e == -1) {                                                      /* new term */
		int textLen1 = 1 + dataLen;
		if (textLen1 + termPool->byteUpto > BYTE_BLOCK_SIZE) {
			termPool->nextBuffer();
		}
		char *text = termPool->buffer;
		int textUpto = termPool->byteUpto;
		if (count > 0 && (count % 10240) == 0) {
			nextBuffer();
		}
		e = count++;                                                    /* termId */
		RawPostingList* postlist = getPostList(e);
		postlist->textStart = textUpto + termPool->byteOffset;
		postlist->termLength = (short)textLen1;
		termPool->byteUpto += textLen1;
		text[textUpto] = field;                                         /* write fieldId into termPool */
		memcpy(text + textUpto + 1, data, dataLen);                     /* write data into termPool */
		assert(current->ords[hashPos] == -1);
		current->ords[hashPos] = e;                                     /* store termId into ords */
		if (count == current->hashHalfSize) {
			rehashPostings(2 * current->hashSize);
		}
	}
	return e;
}

int TermDict::getTerm(TermInfo terminfo) {
	char *data = terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = termHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];
	if (e != -1 && !postingEquals(e, field, data, dataLen)) {
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !postingEquals(e, field, data, dataLen));
	}
	return e;
}

int TermDict::getTerm(Term terminfo) {                                     /* return termId of terminfo */
	char *data = (char*)terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = (char)terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = termHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];                                        /* get termId */
	if (e != -1 && !postingEquals(e, field, data, dataLen)) {
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !postingEquals(e, field, data, dataLen));
	}
	return e;	
}

int TermDict::addDnfTerm(TermInfo terminfo) {               /* new term: add term to dnftermPool, dnfpositions, dnftermHash, return termId; old term: return termId */
	char *data = terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = dnftermHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];
	if (e != -1 && !dnfpostingEquals(e, field, data, dataLen)) {                     /* hash collision */
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !dnfpostingEquals(e, field, data, dataLen));
	}
	if (e == -1) {                                                                   /* new term */
		int textLen1 = 1 + dataLen;
		if (textLen1 + dnftermPool->byteUpto > BYTE_BLOCK_SIZE) {
			dnftermPool->nextBuffer();
		}
		char *text = dnftermPool->buffer;
		int textUpto = dnftermPool->byteUpto;
		if (dnfcount > 0 && (dnfcount % 10240) == 0) {
			nextDnfBuffer();
		}
		e = dnfcount++;                                                              /* term Id */
		DnfRawPostingList* postlist = getDnfPostList(e);
		postlist->textStart = textUpto + dnftermPool->byteOffset;                    /* construct postlist */
		dnftermPool->byteUpto += textLen1;
		text[textUpto] = field;                                                      /* write conjunction size */
		memcpy(text + textUpto + 1, data, dataLen);                                  /* write term data */
		assert(current->ords[hashPos] == -1);
		current->ords[hashPos] = e;                                                  /* store termId into ords */
		if (dnfcount == current->hashHalfSize) {
			rehashDnfPostings(2 * current->hashSize);
		}
	}
	return e;                                                                        /* return termId */
}

int TermDict::getDnfTerm(TermInfo terminfo) {
	char *data = terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = dnftermHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];
	if (e != -1 && !dnfpostingEquals(e, field, data, dataLen)) {
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !dnfpostingEquals(e, field, data, dataLen));
	}
	return e;
}

int TermDict::getDnfTerm(Term terminfo) {
	char *data = (char*)terminfo.data;
	int dataLen = terminfo.dataLen;
	if (data == NULL || dataLen <= 0) return -1;
	char field = (char)terminfo.field;
	int code = Utils::termHash(terminfo);
	TermHash *current = dnftermHash;
	int hashPos = code & current->hashMask;
	int e = current->ords[hashPos];
	if (e != -1 && !dnfpostingEquals(e, field, data, dataLen)) {
		int inc = ((code >> 8) + code) | 1;
		do {
			code += inc;
			hashPos = code & current->hashMask;
			e = current->ords[hashPos];
		} while (e != -1 && !dnfpostingEquals(e, field, data, dataLen));
	}
	return e;
	
}

void TermDict::nextBuffer() {                                 /* add an array of 10240 to positions */
	int i = 0, j = 0;
	if (count > 0 && (count % 10240) == 0) {
		RawPostingList **newpositions = (RawPostingList**)malloc(sizeof(RawPostingList*) * (positionNum + 1));
		for (i = 0; i < positionNum; i++) {
			newpositions[i] = positions[i];
		}
		newpositions[i] = (RawPostingList*)malloc(sizeof(RawPostingList) * 10240);
		bzero(newpositions[i], sizeof(RawPostingList) * 10240);
		for (j = 0; j < 10240; j++) {
			newpositions[i][j].invertedStart = -1;
		}
		gc->addPointer(positions);
		positions = newpositions;
		positionNum += 1;
	}
}

void TermDict::nextDnfBuffer() {                              /* add an array of 10240 to dnfpositions */
	int i = 0, j = 0;
	if (dnfcount > 0 && (dnfcount % 10240) == 0) {
		DnfRawPostingList **newpositions = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*) * (dnfpositionNum + 1));
		for (i = 0; i < dnfpositionNum; i++) {                                                     /* copy content */
			newpositions[i] = dnfpositions[i];
		}
		newpositions[i] = (DnfRawPostingList*)malloc(sizeof(DnfRawPostingList) * 10240);           /* alloc buffer */
		bzero(newpositions[i], sizeof(DnfRawPostingList) * 10240);
		for (j = 0; j < 10240; j++) {
			newpositions[i][j].invertedStart = -1;
		}
		gc->addPointer(dnfpositions);
		dnfpositions = newpositions;
		dnfpositionNum += 1;
	}
}

RawPostingList* TermDict::getPostList(int termId) {
	if (termId < 0 || termId >= count) return NULL;
	RawPostingList* buffer = positions[termId / 10240];
	return buffer + (termId % 10240);
}

DnfRawPostingList* TermDict::getDnfPostList(int termId) {                               /* return posting list of termId */
	if (termId < 0 || termId >= dnfcount) return NULL;
	DnfRawPostingList* buffer = dnfpositions[termId / 10240];                           /* find array in dnfpositions */
	return buffer + (termId % 10240);
}

bool TermDict::postingEquals(int e, char field, char* tokenText, int tokenTextLen) {
	RawPostingList* p = getPostList(e);
	assert(p != NULL);
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

bool TermDict::dnfpostingEquals(int e, char field, char* tokenText, int tokenTextLen) {            /* compare field and data */
	DnfRawPostingList* dnfp = getDnfPostList(e);
	assert(dnfp != NULL);
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

void TermDict::rehashPostings(int newSize) {
	int newMask = newSize - 1, i = 0, j = 0;
	int *newHash = (int*)malloc(sizeof(int) * newSize);
	for (i = 0; i < newSize; i++) {
		newHash[i] = -1;
	}
	TermHash *current = termHash;
	for (i = 0; i < current->hashSize; i++) {
		int e = current->ords[i];
		if (e == -1) continue;
		RawPostingList *p0 = getPostList(e);
		assert(p0 != NULL);
		int code = 0;
		int start = p0->textStart & BYTE_BLOCK_MASK;
		short textLen = p0->termLength;
		char *text = termPool->buffers[p0->textStart >> BYTE_BLOCK_SHIFT];
		for (j = textLen; j > 0; j--) {
			code = (code * 31) + text[start + j - 1];
		}
		int hashPos = code & newMask;
		assert(hashPos >= 0);
		if (newHash[hashPos] != -1) {
			int inc = ((code >> 8) + code) | 1;
			do {
				code += inc;
				hashPos = code & newMask;
			} while (newHash[hashPos] != -1);
		}
		newHash[hashPos] = e;
	}
	TermHash *newTermHash = (TermHash*)malloc(sizeof(TermHash));
	newTermHash->hashMask = newMask;
	newTermHash->ords = newHash;
	newTermHash->hashSize = newSize;
	newTermHash->hashHalfSize = newSize >> 1;
	gc->addPointer(termHash->ords);
	gc->addPointer(termHash);
	termHash = newTermHash;
}

void TermDict::rehashDnfPostings(int newSize) {                                /* expand dnftermHash to new size */
	int newMask = newSize - 1, i = 0, j = 0;
	int *newHash = (int*)malloc(sizeof(int) * newSize);
	for (i = 0; i < newSize; i++) {
		newHash[i] = -1;
	}
	TermHash *current = dnftermHash;
	for (i = 0; i < current->hashSize; i++) {                                  /* write data of old termHash into new termHash */
		int e = current->ords[i];
		if (e == -1) continue;
		DnfRawPostingList *p0 = getDnfPostList(e);
		assert(p0 != NULL);
		int code = 0;
		int start = p0->textStart & BYTE_BLOCK_MASK;
		short textLen = 9;
		char *text = dnftermPool->buffers[p0->textStart >> BYTE_BLOCK_SHIFT];
		for (j = textLen; j > 0; j--) {
			code = (code * 31) + text[start + j - 1];
		}
		int hashPos = code & newMask;                                          /* find hash position in new dnftermHash */
		assert(hashPos >= 0);
		if (newHash[hashPos] != -1) {
			int inc = ((code >> 8) + code) | 1;
			do {
				code += inc;
				hashPos = code & newMask;
			} while (newHash[hashPos] != -1);
		}
		newHash[hashPos] = e;                                                  /* store termId */
	}
	TermHash *newTermHash = (TermHash*)malloc(sizeof(TermHash));
	newTermHash->hashMask = newMask;
	newTermHash->ords = newHash;
	newTermHash->hashSize = newSize;
	newTermHash->hashHalfSize = newSize >> 1;
	gc->addPointer(dnftermHash->ords);
	gc->addPointer(dnftermHash);
	dnftermHash = newTermHash;
}

RamFile::RamFile(SimpleGc *_gc) {
	gc = _gc;
	bufferSize = 10;
	dataBuffer = (char**)malloc(sizeof(char*) * bufferSize);
	bufferLen = 0;
	length = 0;
	sizeInBytes = 0;
}

RamFile::~RamFile() {
	int i = 0;
	if (dataBuffer != NULL) {
	    if (bufferLen > 0) {
		    for (i = 0; i < bufferLen; i++) {
			    char *buffer = dataBuffer[i];
			    free(buffer);
				dataBuffer[i] = NULL;
		    }
	    }
	    free(dataBuffer);
		dataBuffer = NULL;
	}
}

long RamFile::getLength() {
	return length;
}

void RamFile::setLength(long _length) {
	length = _length;
}

char* RamFile::addBuffer(int size) {                                                       /* add a buffer of size 'size' */
	if (size <= 0) return NULL;
	char *buffer = newBuffer(size);
	if (buffer == NULL) return NULL;
	if (bufferLen == bufferSize) {                                                         /* dataBuffer used up, expand it */
		int newbufferSize = bufferSize * 2;
		char **newDataBuffer = (char**)malloc(sizeof(char*) * newbufferSize);
		bzero(newDataBuffer, sizeof(char*) * newbufferSize);
		memcpy(newDataBuffer, dataBuffer, sizeof(char*) * bufferLen);
		gc->addPointer(dataBuffer);
		dataBuffer = newDataBuffer;
		bufferSize *= 2;
	}
	dataBuffer[bufferLen++] = buffer;
	sizeInBytes += size;
	return buffer;
}

char* RamFile::getBuffer(int index) {
	if (index < 0 || index >= bufferLen) return NULL;
	return dataBuffer[index];
}

int RamFile::numBuffers() {
	return bufferLen;
}

char* RamFile::newBuffer(int size) {
	if (size <= 0) return NULL;
	char* buffer = (char*)malloc(sizeof(char) * size);
	return buffer;
}

long RamFile::getSizeInBytes() {
	return sizeInBytes;
}

RAMOutputStream::RAMOutputStream(SimpleGc *gc) {
	currentBuffer = NULL;
	currentBufferIndex = -1;
	bufferPosition = 0;
	bufferStart = 0;
	bufferLength = 0;
	ramfile = new RamFile(gc);
}

RAMOutputStream::~RAMOutputStream() {
	delete ramfile;
}

int RAMOutputStream::writeFile(FILE *file) {                                        /* write data of RamFile into file */
	flush();
    long end = ramfile->getLength();
    long pos = 0;
    int buffer = 0, res = 0;
    while (pos < end) {
		int length = RAMFILE_BUFFER_SIZE;                                           /* write one buffer of RamFile a time */
		long nextPos = pos + length;
		if (nextPos > end) {
			length = (int)(end - pos);                                              /* calculate write length */
		}
		res = fwrite(ramfile->getBuffer(buffer++), sizeof(char), length, file);     /* write */
		if (res != length) return 0;
		pos = nextPos;
    }
	return 1;
}

void RAMOutputStream::reset() {
	currentBuffer = NULL;
    currentBufferIndex = -1;
    bufferPosition = 0;
    bufferStart = 0;
    bufferLength = 0;
    ramfile->setLength(0);
}

void RAMOutputStream::close() {
	flush();
}

void RAMOutputStream::seek(long pos) {                                                   /* set write position to pos, pos is a total shift, like bufferStart */
	if (pos < 0) return;
	setFileLength();
    if (pos < bufferStart || pos >= bufferStart + bufferLength) {                        /* not in the same buffer, switch buffer */
		currentBufferIndex = (int) (pos / RAMFILE_BUFFER_SIZE);
		switchCurrentBuffer();
    }
    bufferPosition = (int) (pos % RAMFILE_BUFFER_SIZE);                                  /* switch position in buffer */
}

long RAMOutputStream::length() {
	return ramfile->getLength();
}

void RAMOutputStream::writeByte(char b) {                                                /* write byte into RamFile */
	if (bufferPosition == bufferLength) {                                                /* reach end of currentBuffer */
		currentBufferIndex++;
		switchCurrentBuffer();
    }
    currentBuffer[bufferPosition++] = b;
}

void RAMOutputStream::writeBytes(char *b, int offset, int len) {                          /* write bytes into RamFile */
	if (b == NULL) return;
	while (len > 0) {
		if (bufferPosition ==  bufferLength) {                                            /* reach end of currentBuffer */
			currentBufferIndex++;
			switchCurrentBuffer();
		}
		int remainInBuffer = RAMFILE_BUFFER_SIZE - bufferPosition;
		int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;                    /* could be not enough room in currentBuffer */
		memcpy(currentBuffer + bufferPosition, b + offset, sizeof(char) * bytesToCopy);
		offset += bytesToCopy;
		len -= bytesToCopy;
		bufferPosition += bytesToCopy;
    }
}

void RAMOutputStream::flush() {                                                 /* update RamFile.length */
	setFileLength();
}

long RAMOutputStream::getFilePointer() {                                        /* return the total shift of position to write in RamFile.dataBuffer */
	return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
}

long RAMOutputStream::sizeInBytes() {                                           /* RamFile.sizeInBytes */
	return (long)ramfile->numBuffers() * (long)RAMFILE_BUFFER_SIZE;
}

void RAMOutputStream::switchCurrentBuffer() {                                   /* switch currentBuffer to RamFile.dataBuffer[currentBufferIndex] */
	if (currentBufferIndex == ramfile->numBuffers()) {
		currentBuffer = ramfile->addBuffer(RAMFILE_BUFFER_SIZE);
    } else {
		currentBuffer = ramfile->getBuffer(currentBufferIndex);
    }
    bufferPosition = 0;
    bufferStart = (long)RAMFILE_BUFFER_SIZE * (long)currentBufferIndex;
    bufferLength = RAMFILE_BUFFER_SIZE;
}

void RAMOutputStream::setFileLength() {                                         /* update RamFile.length */
	long pointer = bufferStart + bufferPosition;
    if (pointer > ramfile->getLength()) {
		ramfile->setLength(pointer);
    }
}

RAMInputStream::RAMInputStream(RamFile *_ramfile) {
	ramfile = _ramfile;
	filelength = ramfile->getLength();
	currentBufferIndex = -1;
    currentBuffer = NULL;
	bufferPosition = 0;
	bufferStart = 0;
	bufferLength = 0;
}

RAMInputStream::~RAMInputStream() {
}

void RAMInputStream::close() {
}

long RAMInputStream::length() {
	return ramfile->getLength();
}

int RAMInputStream::seekNow(long pos) {                                                       /* set read position to pos */
	long nowLen = length();
	if (pos < nowLen) {
	    currentBufferIndex = (int)(pos / RAMFILE_BUFFER_SIZE);
		currentBuffer = ramfile->getBuffer(currentBufferIndex);
	    bufferPosition = (int)(pos % RAMFILE_BUFFER_SIZE);
		bufferStart = currentBufferIndex * RAMFILE_BUFFER_SIZE;
	    long buflen = nowLen - bufferStart;
	    bufferLength = buflen > RAMFILE_BUFFER_SIZE ? RAMFILE_BUFFER_SIZE : (int) buflen;     /* get length of data bytes in currentBuffer, might be less than RAMFILE_BUFFER_SIZE */
		return 1;
	} else return 0;                                                                          /* out of scope */
}

char RAMInputStream::readByteNow() {
	return currentBuffer[bufferPosition++];
}

int RAMInputStream::readBytesNow(char *b, int offset, int len) {                              /* return: read bytes, might be less than len */
	int num = 0;
	int remainInBuffer = bufferLength - bufferPosition;                                       /* unread bytes left in currentBuffer */
	int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
	memcpy(b + offset, currentBuffer + bufferPosition, sizeof(char) * bytesToCopy);
	offset += bytesToCopy;
	num = bytesToCopy;
	bufferPosition += bytesToCopy;
	return num;
}

char RAMInputStream::readByte() {                                                              /* safe way to read byte */
	if (bufferPosition >= bufferLength) {
		currentBufferIndex++;
		switchCurrentBuffer(true);
    }
    return currentBuffer[bufferPosition++];
}

int RAMInputStream::readBytes(char *b, int offset, int len) {                                  /* return: read bytes, must be len */
	int num = 0;
	while (len > 0) {                                                                          /* TODO: if total unread bytes is less than len, this could be a endless loop */
		if (bufferPosition >= bufferLength) {                                                  /* finish reading this buffer, move to next */
			currentBufferIndex++;
			switchCurrentBuffer(true);                                                         /* if no buffer left, this function does nothing, could cause endless loop */
		}
		int remainInBuffer = bufferLength - bufferPosition;
		int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
		memcpy(b + offset, currentBuffer + bufferPosition, sizeof(char) * bytesToCopy);
		offset += bytesToCopy;
		num += bytesToCopy;
		len -= bytesToCopy;
		bufferPosition += bytesToCopy;
    }
	return num;
}

long RAMInputStream::getFilePointer() {
	return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
}

void RAMInputStream::seek(long pos) {
	if (currentBuffer == NULL || pos < bufferStart || pos >= bufferStart + RAMFILE_BUFFER_SIZE) {
		currentBufferIndex = (int)(pos / RAMFILE_BUFFER_SIZE);
		switchCurrentBuffer(false);
    }
    bufferPosition = (int)(pos % RAMFILE_BUFFER_SIZE);
}

int RAMInputStream::switchCurrentBuffer(bool enforceEOF) {
	bufferStart = (long)RAMFILE_BUFFER_SIZE * (long)currentBufferIndex;
    if (currentBufferIndex >= ramfile->numBuffers()) {
		if (enforceEOF) {                                                                    /* if enforceEOF, do nothing */
			return 0;
		} else {
			currentBufferIndex--;
			bufferPosition = RAMFILE_BUFFER_SIZE;
		}
    } else {
		currentBuffer = ramfile->getBuffer(currentBufferIndex);
		bufferPosition = 0;
		long buflen = length() - bufferStart;
		bufferLength = buflen > RAMFILE_BUFFER_SIZE ? RAMFILE_BUFFER_SIZE : (int) buflen;
    }
	return 1;
}

Realtime::Realtime(void *_conf) {
	Conf_t *conf = (Conf_t*)_conf;
	schema = conf->schema;
	fullDelRef = conf->fullDelRef;
	incrDelRef = conf->incrDelRef;
	indexPath = conf->realtimeindex;
	keyField = schema->getPrimary();
	parser = new DocParser(schema);
	char *dicPath = conf->realtimedict;
	Utils::rmdirectory(dicPath);                                                                  /* clear folders */
	Utils::mkdirectory(dicPath);
	Utils::rmdirectory(indexPath);
	Utils::mkdirectory(indexPath);
	int num = schema->size(), i = 0;
	brifeOutput = (RAMOutputStream**)malloc(sizeof(RAMOutputStream*) * num);
	bzero(brifeOutput, sizeof(RAMOutputStream*) * num);
	indexFieldNumber = 0;
	indexField = (IndexField**)malloc(sizeof(IndexField*) * num);
	IndexField **fieldArray = schema->toArray();
	analyzers = (Analyzer**)malloc(sizeof(Analyzer*) * num);
	storeFieldNumber = 0;
	storeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	brifeFieldNumber = 0;
    brifeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	gc = new SimpleGc();
	readyflag = true;
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isIndex()) {
			indexField[indexFieldNumber] = get;
			int indexType = get->getType();
			if (indexType == 0) {                                                               /* int */
				analyzers[indexFieldNumber] = new TextAnalyzer(get);
			} else if (indexType == 1) {                                                        /* str */
				analyzers[indexFieldNumber] = new TextAnalyzer(get);
			} else if (indexType == 2) {                                                        /* dnf */
				analyzers[indexFieldNumber] = new DnfAnalyzer(get, dicPath);
				if (!((DnfAnalyzer*)analyzers[indexFieldNumber])->ready()) readyflag = false;
			} else if (indexType == 3) {                                                        /* tag */
				analyzers[indexFieldNumber] = new TagsAnalyzer(get);
			} else {
				exit(-1);
			}
			indexFieldNumber++;
		}
		if (get->isStore()) {
			storeField[storeFieldNumber++] = get;
		}
		if (get->isFilter() && get->getLength() > 0) {
			brifeField[brifeFieldNumber++] = get;
			brifeOutput[i] = new RAMOutputStream(gc);
		} else {
			brifeOutput[i] = NULL;
		}
	}
	storeLenOutput = new RAMOutputStream(gc);
	storePosOutput = new RAMOutputStream(gc);
	storeDatOutput = new RAMOutputStream(gc);
	storeBuffer = (char*)malloc(sizeof(char) * MAX_STORE_LENGTH);
	storeLen = 0;
	bitmapLen = 1;
	bitmap = (char**)malloc(sizeof(char*));
	bitmap[0] = (char*)malloc(REALTIME_BITMAP_LEN);
	bzero(bitmap[0], REALTIME_BITMAP_LEN);
	infos = (TermInfo**)malloc(sizeof(TermInfo*) * MAX_INDEX_FIELD);
	infoLens = (int*)malloc(sizeof(int) * MAX_INDEX_FIELD);
	infoNum = 0;
	allocator = new Allocator();
	termPool = new ByteBlockPool(allocator, gc);
	termPostPool = new ByteBlockPool(allocator, gc);
	dnftermPool = new ByteBlockPool(allocator, gc);
	dnftermPostPool = new ByteBlockPool(allocator, gc);
	dnfconjPostPool = new ByteBlockPool(allocator, gc);
	dnfconjPostIndex = new IntBlockPool(allocator, gc);
	termDict = new TermDict(termPool, dnftermPool, gc);
    int conjStart = dnfconjPostPool->newRtSlice(ByteBlockPool::RT_FIRST_LEVEL_SIZE);                       /* write data of conjId 0 */
	if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
		dnfconjPostIndex->nextBuffer();
	}
	dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
	dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;
	time_t t;
	t = time(&t);
	lastGc = (int)t;
	TermInfo emptyTerm;
	char *temp = (char*)malloc(sizeof(char) * 10);
	bzero(temp, sizeof(char) * 10);
	temp[8] = 1;                                                                                            /* belong */
	emptyTerm.data = temp;
	emptyTerm.dataLen = 8;
	emptyTerm.payload = temp + 8;
	emptyTerm.payloadLen = 1;
	emptyTerm.field = 0;                                                                                    /* conjSize: 0 */
	int termId = termDict->addDnfTerm(emptyTerm);                                                           /* add empty dnf term */
	dnfp = termDict->getDnfPostList(termId);
	if (BYTE_BLOCK_SIZE - dnftermPostPool->byteUpto < ByteBlockPool::RT_FIRST_LEVEL_SIZE) {
		dnftermPostPool->nextBuffer();
	}
	int upto = dnftermPostPool->newRtSlice(ByteBlockPool::RT_FIRST_LEVEL_SIZE);                              /* alloc slice in dnftermPostPool */
	dnfp->invertedStart = upto + dnftermPostPool->byteOffset;                                                /* write invertedStart */
	dnftermVInt(0);                                                                                          /* write conjId 0 in dnftermPostPool */
	int payi = 0;
	for (payi = 0; payi < emptyTerm.payloadLen; payi++) {                                                    /* write payload(belong) in dnftermPostPool */
		dnftermByte(emptyTerm.payload[payi]);
	}
	free(temp);
	docnum = 0;
	conjnum = 1;
	maxconjsize = 0;
	reader = new RtByteSliceReader();
}

Realtime::~Realtime() {
	if (indexField != NULL) {
		free(indexField);
	    indexField = NULL;
	}
	int i = 0;
	if (storeField != NULL) {
	    free(storeField);
		storeField = NULL;
	}
	if (brifeField != NULL) {
	    free(brifeField);
		brifeField = NULL;
	}
	int num = schema->size();
	if (brifeOutput != NULL) {
		for (i = 0; i < num; i++) {
			RAMOutputStream *output = brifeOutput[i];
			if (output != NULL) delete output;
		}
	    free(brifeOutput);
		brifeOutput = NULL;
	}
	if (storeLenOutput != NULL) {
		delete storeLenOutput;
		storeLenOutput = NULL;
	}
	if (storePosOutput != NULL) {
		delete storePosOutput;
		storePosOutput = NULL;
	}
	if (storeDatOutput != NULL) {
		delete storeDatOutput;
		storeDatOutput = NULL;
	}
	if (bitmap != NULL) {
	    for (i = 0; i < bitmapLen; i++) {
		    if (bitmap[i] != NULL) free(bitmap[i]);
			bitmap[i] = NULL;
	    }
	    free(bitmap);
	    bitmap = NULL;
	}
	if (storeBuffer != NULL) {
		free(storeBuffer);
		storeBuffer = NULL;
	}
	if (infos != NULL) {
		free(infos);
		infos = NULL;
	}
	if (infoLens != NULL) {
		free(infoLens);
		infoLens = NULL;
	}
	if (termPool != NULL) {
		delete termPool;
		termPool = NULL;
	}
	if (termPostPool != NULL) {
		delete termPostPool;
		termPostPool = NULL;
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
	if (termDict != NULL) {
	    delete termDict;
		termDict = NULL;
	}
	if (allocator != NULL) {
		delete allocator;
		allocator = NULL;
	}
	if (gc != NULL) {
		delete gc;
		gc = NULL;
	}
	if (reader != NULL) {
		delete reader;
		reader = NULL;
	}
	if (parser != NULL) {
		delete parser;
		parser = NULL;
	}
}

int Realtime::ready() {
	return readyflag ? 1 : 0;
}

bool Realtime::isDeleted(int docid) {
	if (docid >= REALTIME_BITMAP_LEN * bitmapLen * 8) return false;
	char *bitArr = bitmap[(docid / 8) / REALTIME_BITMAP_LEN];
	char bit = bitArr[(docid / 8) % REALTIME_BITMAP_LEN];
	return ((bit >> (7 - docid % 8)) & (char)1) == (char)1;
}

void Realtime::markDeleted(int docid) {                                                   /* set delete mark in bitmap */
	if (docid >= REALTIME_BITMAP_LEN * bitmapLen * 8) return;
	char *bitArr = bitmap[(docid / 8) / REALTIME_BITMAP_LEN];
	bitArr[(docid / 8) % REALTIME_BITMAP_LEN] |= (char)1 << (7 - docid % 8);
}

int Realtime::close() {
	int i = 0, res = 1;
	res = saveBrife();
	if (res <= 0) return res;
	res = saveStore();
	if (res <= 0) return res;
	res = saveIndex();
	if (res <= 0) return res;
	res = saveDnfIndex();
	if (res <= 0) return res;
	res = saveConjuction();
	if (res <= 0) return res;
	res = saveDelete();
	if (analyzers != NULL) {
		for (i = 0; i < indexFieldNumber; i++) {
		    if (analyzers[i] != NULL) delete analyzers[i];
		    analyzers[i] = NULL;
	    }
	    free(analyzers);
	    analyzers = NULL;
	}
	return res;
}

int Realtime::saveDelete() {                                                           /* write del.dat in realtime, full, incr */
	char tpfile_index[MAX_DIR_LEN];
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, INDEX_DEL);
	FILE *fp = NULL;
	fp = fopen(tpfile_index, "w");                                                               /* open del.dat in realtime */
	if (fp == NULL) return 0;
	int i = 0, num = (docnum + 7) / 8, res = 0;
	bool flag = true;
	while (num > 0) {                                                                            /* write bitmap into del.dat */
		if (num > REALTIME_BITMAP_LEN) {
			res = fwrite(bitmap[i], sizeof(char), REALTIME_BITMAP_LEN, fp);
			if (res != REALTIME_BITMAP_LEN) {
				flag = false;
				break;
			}
		} else {
			res = fwrite(bitmap[i], sizeof(char), num, fp);
			if (res != num) flag = false;
			break;
		}
		i++;
		num -= REALTIME_BITMAP_LEN;
	}
	if (fp != NULL) fclose(fp);
	if (fullDelRef != NULL && fullDelRef->filePath != NULL) {
		fp = fopen(fullDelRef->filePath, "w");
		if (fp != NULL) {
			res = fwrite(fullDelRef->bitdata, sizeof(char), fullDelRef->length, fp);            /* update del.dat in full-index */
			if (res != fullDelRef->length) flag = 0;
			fclose(fp);
		} else flag = 0;
	}
	if (incrDelRef != NULL && incrDelRef->filePath != NULL) {
		fp = fopen(incrDelRef->filePath, "w");
		if (fp != NULL) {
			res = fwrite(incrDelRef->bitdata, sizeof(char), incrDelRef->length, fp);            /* update del.dat in incr-index */
			if (res != incrDelRef->length) flag = 0;
			fclose(fp);
		} else flag = 0;
	}
	return flag ? 1 : 0;
}

int Realtime::saveBrife() {                                                                     /* write brifeOutput to disk */
	char tpfile_index[MAX_DIR_LEN];
	IndexField **fieldArray = schema->toArray();
	int num = schema->size(), i = 0, res = 0;
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isFilter() && get->getLength() > 0) {
			snprintf(tpfile_index, MAX_DIR_LEN, "%s%s.dat", indexPath, get->getName());
			FILE *brifefp = NULL;
			brifefp = fopen(tpfile_index, "w");                                                 /* open file "fieldname.dat" */
			if (brifefp == NULL) return 0;
			RAMOutputStream *output = brifeOutput[i];
			res = output->writeFile(brifefp);                                                   /* write */
			if (res <= 0) return 0;
			fclose(brifefp);
		}
	}
	return 1;
}

int Realtime::saveStore() {                                                                     /* write storeDatOutput, storePosOutput, storeLenOutput to disk */
	char tpfile[MAX_DIR_LEN];
	int res = 0;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", indexPath, STORE_POS_FILE_NAME);
	FILE *fp = NULL;
	fp = fopen(tpfile, "w");                                                                    /* open store.pos */
	if (fp == NULL) return 0;
	res =  storePosOutput->writeFile(fp);                                                       /* write */
	if (res <= 0) return 0;
	fclose(fp);
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", indexPath, STORE_LEN_FILE_NAME);
	fp = fopen(tpfile, "w");                                                                    /* open store.len */
	if (fp == NULL) return 0;
	res =  storeLenOutput->writeFile(fp);                                                       /* write */
	if (res <= 0) return 0;
	fclose(fp);
	snprintf(tpfile, MAX_DIR_LEN, "%s%s.0", indexPath, STORE_FILE_NAME);
	fp = fopen(tpfile, "w");                                                                    /* open store.dat.0 */
	if (fp == NULL) return 0;
	res =  storeDatOutput->writeFile(fp);                                                       /* write */
	if (res <= 0) return 0;
	fclose(fp);
	return 1;
}

int Realtime::saveIndex() {                                                                    /* write term.idx, term.dat, index.dat */
	int *newwords = compact(termDict->termHash, termDict->count);
	if (newwords != NULL) {
		sortposting(newwords, 0, termDict->count - 1);
	}
	int i = 0, docNum = 0, writeLen = 0, termWriteLen = 0, res = 0;
	int vint = 0;
	long vlong = 0, indexWrite = 0;
	//char *writeBuffer = (char*)malloc(sizeof(char) * 600000);
	//char *termWriteBuffer = (char*)malloc(sizeof(char) * 200);
    char *writeBuffer = (char*)malloc(sizeof(char) * 610000);
    bzero(writeBuffer, sizeof(char) * 610000);
    char *termWriteBuffer = (char*)malloc(sizeof(char) * 200);
    bzero(termWriteBuffer, sizeof(char) * 200);
	FILE *indexfp, *termfp;
	char tpfile_index[MAX_DIR_LEN];
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, INDEX_INVERT_DAT);
	indexfp = fopen(tpfile_index, "w");                                                         /* open index.dat */
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, INDEX_TERM_DAT);
	termfp = fopen(tpfile_index, "w");                                                          /* open term.dat */
	int termNum = 0;
	long termWrite = 0;
	FILE *termskipfp;
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, INDEX_TERM_SKIP);
	termskipfp = fopen(tpfile_index, "w");                                                      /* open term.idx */
	bool flag = true;
	if (termfp == NULL || indexfp == NULL || termskipfp == NULL) flag = false;
	if (flag) {
	    vint = 1;                                                                               /* version */
		termWriteBuffer[0] = (char)(vint >> 0);
		termWriteBuffer[1] = (char)(vint >> 8);
		termWriteBuffer[2] = (char)(vint >> 16);
		termWriteBuffer[3] = (char)(vint >> 24);
		vint = termDict->count;                                                                 /* term count */
		termWriteBuffer[4] = (char)(vint >> 0);
		termWriteBuffer[5] = (char)(vint >> 8);
		termWriteBuffer[6] = (char)(vint >> 16);
		termWriteBuffer[7] = (char)(vint >> 24);
		res = fwrite(termWriteBuffer, sizeof(char), 8, termfp);
		if (res != 8) flag = false;
		vint = (termDict->count + 127) / 128 + 1;                                               /* skip-term count */
		termWriteBuffer[4] = (char)(vint >> 0);
		termWriteBuffer[5] = (char)(vint >> 8);
		termWriteBuffer[6] = (char)(vint >> 16);
		termWriteBuffer[7] = (char)(vint >> 24);
		res = fwrite(termWriteBuffer, sizeof(char), 8, termskipfp);
		if (res != 8) flag = false;
	}
	for (i = 0; (i < termDict->count) && flag; i++) {                                           /* iterate all terms */
		RawPostingList *post = termDict->getPostList(newwords[i]);
		if (post == NULL) continue;
		docNum = 0;
		writeLen = 0;
		termWriteLen = 0;
		reader->init(termPostPool, post->invertedStart, post->invertedUpto);
		while (!reader->eof()) {
			char b = reader->readByte();                                                         /* docId */
			int shift = 7;
			int readdoc = b & 0x7F;
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			writeBuffer[writeLen++] = reader->readByte();                                        /* payload */
			writeBuffer[writeLen++] = reader->readByte();
			docNum++;
		}
		vint = post->termLength;
		while ((vint & ~0x7F) != 0) {                                                            /* term length */
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7F) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		char *text1 = termPool->buffers[post->textStart >> BYTE_BLOCK_SHIFT];
		int pos1 = post->textStart & BYTE_BLOCK_MASK;
		memcpy(termWriteBuffer + termWriteLen, text1 + pos1, post->termLength);                  /* term field+data */
		termWriteLen += post->termLength;
		vint = docNum;                                                                           /* doc count */
		while ((vint & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f) | 0x80);
			vint >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vint & 0x7f));
		vlong = indexWrite;                                                                      /* shift of posting list in index.dat */
		while ((vlong & ~0x7F) != 0) {
			termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f) | 0x80);
			vlong >>= 7;
		}
		termWriteBuffer[termWriteLen++] = (char)((vlong & 0x7f));
		Utils::decode(post->ub, termWriteBuffer + termWriteLen);                                 /* UB */
		termWriteLen += 2;
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termfp);                       /* write term.dat */
		if (res != termWriteLen) flag = false;
		if ((termNum % 128) == 0 || i == (termDict->count - 1)) {                                /* need to write in term.idx */
			vlong = termWrite;                                                                   /* shift of skip-term in term.dat */
			int appendLen = termWriteLen;
			while ((vlong & ~0x7F) != 0) {
				termWriteBuffer[appendLen++] = (char)((vlong & 0x7f) | 0x80);
				vlong >>= 7;
			}
			termWriteBuffer[appendLen++] = (char)((vlong & 0x7f));
			res = fwrite(termWriteBuffer, sizeof(char), appendLen, termskipfp);                  /* write term.idx */
			if (res != appendLen) flag = false;
		}
		termNum++;
		termWrite += termWriteLen;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexfp);                              /* write index.dat */
		if (res != writeLen) flag = false;
		indexWrite += writeLen;
	}
	if (indexfp != NULL) fclose(indexfp);
	if (termfp != NULL) fclose(termfp);
	if (termskipfp != NULL) fclose(termskipfp);
	free(writeBuffer);
	free(termWriteBuffer);
	if (newwords != NULL) free(newwords);
	return flag ? 1 : 0;
}

int* Realtime::compact(TermHash *current, int count) {                              /* store termId in TermHash into newords, return newords */
	int upto = 0, i = 0;
	if (count <= 0) return NULL;
	int *newords = (int*)malloc(sizeof(int) * count);
	for (i = 0; i < current->hashSize; i++) {
		if (current->ords[i] != -1) {
			newords[upto] = current->ords[i];
			upto++;
		}
	}
	assert(upto == count);
	return newords;
}

void Realtime::sortposting(int *ords, int lo, int hi) {                             /* sort termId in ords in increasing order of term field+data */
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (RawPostingListCmp(ords[lo], ords[hi]) > 0) {
			int tmp = ords[lo];
			ords[lo] = ords[hi];
			ords[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (RawPostingListCmp(ords[lo], ords[mid]) > 0) {
		int tmp = ords[lo];
		ords[lo] = ords[mid];
		ords[mid] = tmp;
	}
	if (RawPostingListCmp(ords[mid], ords[hi]) > 0) {
		int tmp = ords[mid];
		ords[mid] = ords[hi];
		ords[hi] = tmp;
		if (RawPostingListCmp(ords[lo], ords[mid]) > 0) {
			int tmp2 = ords[lo];
			ords[lo] = ords[mid];
			ords[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	int partition = ords[mid];
	for (;;) {
		while (RawPostingListCmp(ords[right], partition) > 0) --right;
		while (left < right && RawPostingListCmp(ords[left], partition) <= 0) ++left;
		if (left < right) {
			int tmp = ords[left];
			ords[left] = ords[right];
			ords[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	sortposting(ords, lo, left);
	sortposting(ords, left + 1, hi);
}

int Realtime::RawPostingListCmp(int a, int b) {                                     /* compare term field+data of termId: a, b */
	if (a == b) return 0;
	RawPostingList *p1 = termDict->getPostList(a);
	RawPostingList *p2 = termDict->getPostList(b);
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

int Realtime::saveDnfIndex() {
	int *newwords = compact(termDict->dnftermHash, termDict->dnfcount);
	if (newwords != NULL) {
		sortdnfposting(newwords, 0, termDict->dnfcount - 1);                                   /* sort */
	}
	int i = 0, docNum = 0, writeLen = 0, termWriteLen = 0, res = 0;
	int vint = 0;
	long vlong = 0, indexWrite = 0;
	char *writeBuffer = (char*)malloc(sizeof(char) * 600000);
	char *termWriteBuffer = (char*)malloc(sizeof(char) * 200);
	FILE *indexfp, *termfp;
	char tpfile_index[MAX_DIR_LEN];
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, DNF_INDEX_INVERT_DAT);
	indexfp = fopen(tpfile_index, "w");                                                        /* open dnfindex.dat */
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, DNF_INDEX_TERM_DAT);
	termfp = fopen(tpfile_index, "w");                                                         /* open dnfterm.dat */
	int termNum = 0;
	FILE *termskipfp;
	snprintf(tpfile_index, MAX_DIR_LEN, "%s%s", indexPath, DNF_INDEX_TERM_SKIP);
	termskipfp = fopen(tpfile_index, "w");                                                     /* open dnfterm.idx */
	bool flag = true;
	if (termfp == NULL || indexfp == NULL || termskipfp == NULL) flag = false;
	if (flag) {
		vint = 1;                                                                              /* version */
		termWriteBuffer[0] = (char)(vint >> 0);
		termWriteBuffer[1] = (char)(vint >> 8);
		termWriteBuffer[2] = (char)(vint >> 16);
		termWriteBuffer[3] = (char)(vint >> 24);
		vint = termDict->dnfcount;                                                             /* dnf term count */
		termWriteBuffer[4] = (char)(vint >> 0);
		termWriteBuffer[5] = (char)(vint >> 8);
		termWriteBuffer[6] = (char)(vint >> 16);
		termWriteBuffer[7] = (char)(vint >> 24);
		res = fwrite(termWriteBuffer, sizeof(char), 8, termfp);
		if (res != 8) flag = false;
		vint = (termDict->dnfcount + 127) / 128 + 1;                                           /* dnf skip-term count */
		termWriteBuffer[4] = (char)(vint >> 0);
		termWriteBuffer[5] = (char)(vint >> 8);
		termWriteBuffer[6] = (char)(vint >> 16);
		termWriteBuffer[7] = (char)(vint >> 24);
		res = fwrite(termWriteBuffer, sizeof(char), 8, termskipfp);
		if (res != 8) flag = false;
	}
	for (i = 0; (i < termDict->dnfcount) && flag; i++) {                                        /* iterate dnf terms */
		DnfRawPostingList *post = termDict->getDnfPostList(newwords[i]);
		if (post == NULL) continue;
		docNum = 0;
		writeLen = 0;
		termWriteLen = 0;
		reader->init(dnftermPostPool, post->invertedStart, post->invertedUpto);
		while (!reader->eof()) {
			char b = reader->readByte();
			int shift = 7;
			int readdoc = b & 0x7F;                                                             /* conjId */
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			writeBuffer[writeLen++] = reader->readByte();                                       /* payload(belong) */
			docNum++;
		}
		char *text1 = dnftermPool->buffers[post->textStart >> BYTE_BLOCK_SHIFT];
		int pos1 = post->textStart & BYTE_BLOCK_MASK;
		memcpy(termWriteBuffer + termWriteLen, text1 + pos1, 9);                                /* term: conjSize+data */
		termWriteLen += 9;
		vint = docNum;                                                                          /* count of conj */
		termWriteBuffer[termWriteLen++] = (char)(vint >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vint >> 24);
		vlong = indexWrite;                                                                     /* shift of posting list in dnfindex.dat */
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 0);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 8);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 16);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 24);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 32);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 40);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 48);
		termWriteBuffer[termWriteLen++] = (char)(vlong >> 56);
		res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termfp);                      /* write dnfterm.dat */
		if (res != termWriteLen) flag = false;
		if ((termNum % 128) == 0 || i == (termDict->dnfcount - 1)) {
			res = fwrite(termWriteBuffer, sizeof(char), termWriteLen, termskipfp);              /* write dnfterm.idx */
			if (res != termWriteLen) flag = false;
		}
		termNum++;
		res = fwrite(writeBuffer, sizeof(char), writeLen, indexfp);                             /* write dnfindex.dat */
		if (res != writeLen) flag = false;
		indexWrite += writeLen;
	}
	if (indexfp != NULL) fclose(indexfp);
	if (termfp != NULL) fclose(termfp);
	if (termskipfp != NULL) fclose(termskipfp);
	free(writeBuffer);
	free(termWriteBuffer);
	if (newwords != NULL) free(newwords);
	return flag ? 1 : 0;
}

void Realtime::sortdnfposting(int *ords, int lo, int hi) {                           /* sort termId in ords in increasing order of term conjSize+data */
	if (lo >= hi) return;
	else if (hi == 1 + lo) {
		if (DnfRawPostingListCmp(ords[lo], ords[hi]) > 0) {
			int tmp = ords[lo];
			ords[lo] = ords[hi];
			ords[hi] = tmp;
		}
		return;
	}
	int mid = (lo + hi) >> 1;
	if (DnfRawPostingListCmp(ords[lo], ords[mid]) > 0) {
		int tmp = ords[lo];
		ords[lo] = ords[mid];
		ords[mid] = tmp;
	}
	if (DnfRawPostingListCmp(ords[mid], ords[hi]) > 0) {
		int tmp = ords[mid];
		ords[mid] = ords[hi];
		ords[hi] = tmp;
		if (DnfRawPostingListCmp(ords[lo], ords[mid]) > 0) {
			int tmp2 = ords[lo];
			ords[lo] = ords[mid];
			ords[mid] = tmp2;
		}
	}
	int left = lo + 1;
	int right = hi - 1;
	if (left >= right) return;
	int partition = ords[mid];
	for (;;) {
		while (DnfRawPostingListCmp(ords[right], partition) > 0) --right;
		while (left < right && DnfRawPostingListCmp(ords[left], partition) <= 0) ++left;
		if (left < right) {
			int tmp = ords[left];
			ords[left] = ords[right];
			ords[right] = tmp;
			--right;
		} else {
			break;
		}
	}
	sortdnfposting(ords, lo, left);
	sortdnfposting(ords, left + 1, hi);
}

int Realtime::DnfRawPostingListCmp(int a, int b) {
	if (a == b) return 0;
	DnfRawPostingList *p1 = termDict->getDnfPostList(a);
	DnfRawPostingList *p2 = termDict->getDnfPostList(b);
	char *text1 = dnftermPool->buffers[p1->textStart >> BYTE_BLOCK_SHIFT];
	int pos1 = p1->textStart & BYTE_BLOCK_MASK;
	char *text2 = dnftermPool->buffers[p2->textStart >> BYTE_BLOCK_SHIFT];
	int pos2 = p2->textStart & BYTE_BLOCK_MASK;
	return memcmp(text1 + pos1, text2 + pos2, 9);
}

int Realtime::saveConjuction() {                                           /* write conj.idx, conj.dat, max.conjsize */
	FILE *indexfp, *conjfp;
	char tpfile[MAX_DIR_LEN];
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", indexPath, CONJUCTION_INDEX);
	indexfp = fopen(tpfile, "w");                                                                       /* open conj.idx */
	snprintf(tpfile, MAX_DIR_LEN, "%s%s", indexPath, CONJUCTION_DAT);
	conjfp = fopen(tpfile, "w");                                                                        /* open conj.dat */
	char *writeBuffer = (char*)malloc(sizeof(char) * 2000);
	char *indexwriteBuffer = (char*)malloc(sizeof(char) * 12);
	bool flag = true;
	if (conjfp == NULL || indexfp == NULL) flag = false;
	int i = 0, docNum = 0, writeLen = 0, res = 0;
	long totalWrite = 0;
	for (i = 0; (i < dnfconjPostIndex->intOffset + dnfconjPostIndex->intUpto) && flag; i = i + 2) {     /* iterate dnfconjPostIndex */
		int index = i;
		int *data = dnfconjPostIndex->buffers[index >> INT_BLOCK_SHIFT];
		int offset = index & INT_BLOCK_MASK;
		reader->init(dnfconjPostPool, data[offset], data[offset + 1]);
		docNum = 0;
		writeLen = 0;
		indexwriteBuffer[0] = (char)(totalWrite >> 0);                                                  /* shift in conj.dat */
        indexwriteBuffer[1] = (char)(totalWrite >> 8);
        indexwriteBuffer[2] = (char)(totalWrite >> 16);
        indexwriteBuffer[3] = (char)(totalWrite >> 24);
        indexwriteBuffer[4] = (char)(totalWrite >> 32);
        indexwriteBuffer[5] = (char)(totalWrite >> 40);
        indexwriteBuffer[6] = (char)(totalWrite >> 48);
        indexwriteBuffer[7] = (char)(totalWrite >> 56);
		while (!reader->eof()) {
			char b = reader->readByte();                                                                /* docId */
			int shift = 7;
			int readdoc = b & 0x7F;
			for (; (b & 0x80) != 0; shift += 7) {
				b = reader->readByte();
				readdoc |= (b & 0x7F) << shift;
			}
			writeBuffer[writeLen++] = (char)(readdoc >> 0);
			writeBuffer[writeLen++] = (char)(readdoc >> 8);
			writeBuffer[writeLen++] = (char)(readdoc >> 16);
			writeBuffer[writeLen++] = (char)(readdoc >> 24);
			if (writeLen > 1996) {
				res = fwrite(writeBuffer, sizeof(char), writeLen, conjfp);
				if (res != writeLen) {
					break;
					flag = false;
				}
				totalWrite += writeLen;
				writeLen = 0;
			}
			docNum++;
		}
		if (!flag) break;
		if (writeLen > 0) {
			res = fwrite(writeBuffer, sizeof(char), writeLen, conjfp);                                  /* write conj.dat */
			if (res != writeLen) {
				break;
				flag = false;
			}
			totalWrite += writeLen;
		}
		indexwriteBuffer[8] = (char)(docNum >> 0);                                                      /* doc count */
        indexwriteBuffer[9] = (char)(docNum >> 8);
        indexwriteBuffer[10] = (char)(docNum >> 16);
        indexwriteBuffer[11] = (char)(docNum >> 24);
		res = fwrite(indexwriteBuffer, sizeof(char), 12, indexfp);                                      /* write conj.idx */
		if (res != 12) {
			break;
			flag = false;
		}
	}
	if (indexfp != NULL) fclose(indexfp);
	if (conjfp != NULL) fclose(conjfp);
	free(writeBuffer);
	free(indexwriteBuffer);
	if (flag) {                                                                                          /* write max.conjsize */
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", indexPath, MAX_CONJSIZE);
		FILE *maxconjfp = NULL;
		maxconjfp = fopen(tpfile, "w");
		if (maxconjfp != NULL) {
			snprintf(tpfile, MAX_DIR_LEN, "%d\n", maxconjsize);
			res = fwrite(tpfile, sizeof(char), strlen(tpfile), maxconjfp);
			if (res != (int)strlen(tpfile)) flag = false;
			fclose(maxconjfp);
		} else flag = false;
	}
	return flag ? 1 : 0;
}

/* parse doc and generate index data, mark delete old doc of uid */
int Realtime::addDoc(char *data, int dataLen) {
	if (data == NULL || dataLen <= 0) return 0;
	bool parserData = parser->initData(data, dataLen);                                                  /* parse doc */
	if (!parserData) {
		return 0;
	}
	int res = 0;
	char *keyData = parser->data(keyField->getNumber());
	int keyDataLen = parser->length(keyField->getNumber());
	if (keyData == NULL || keyDataLen != 4) {
		return 0;
	}
	//int uid = ((keyData[0] & 0xFF)) + ((keyData[1] & 0xFF) << 8)
	//+ ((keyData[2] & 0xFF) <<  16) + (keyData[3] & 0xFF << 24);
	int uid = (keyData[0] & 0xff) + ((keyData[1] & 0xff) << 8) + ((keyData[2] & 0xff) << 16) + ((keyData[3] & 0xff) << 24);
	res = doInverted();                                                                                  /* write index data */
	if (res <= 0) {
		return 0;
	}
	doBrife();                                                                                           /* write brife data */
	doStore();                                                                                           /* write store data */
	docnum++;
	res = doDelete(uid);                                                                                 /* mark delete docId if existed */
	if (res <= 0) {
		return 0;
	}
	uidmap.erase(uid);
	uidmap.insert(pair<int, int>(uid, docnum - 1));                                                      /* write docId of doc(uid) */
	if (incrDelRef != NULL) {                                                                            /* mark delete in incr */
		char *bitdata = incrDelRef->bitdata;
		int length = incrDelRef->length;
		DocIDMapper *mapper = incrDelRef->mapper;
		if (bitdata != NULL && length > 0 && mapper != NULL) {
			int docid = mapper->getDocID((long)uid);
			//printf("incr %d %d\n", uid, docid);
			if (docid >= 0 && (docid + 8) / 8 <= length) {
				bitdata[(docid / 8)] |= (char)1 << (7 - docid % 8);
			}
		}
	}
	if (fullDelRef != NULL) {                                                                            /* mark delete in full */
		char *bitdata = fullDelRef->bitdata;
		int length = fullDelRef->length;
		DocIDMapper *mapper = fullDelRef->mapper;
		if (bitdata != NULL && length > 0 && mapper != NULL) {
			int docid = mapper->getDocID((long)uid);
			if (docid >= 0 && (docid + 8) / 8 <= length) {
				bitdata[(docid / 8)] |= (char)1 << (7 - docid % 8);
			}
		}
	}
	conjnum = (dnfconjPostIndex->intOffset + dnfconjPostIndex->intUpto) / 2;
	if (docnum == REALTIME_BITMAP_LEN * bitmapLen * 8) {                                                 /* expand bitmap */
		int newBitmapLen = bitmapLen + 1;
		char **newbitmap = (char**)malloc(sizeof(char*) * newBitmapLen);
		memcpy(newbitmap, bitmap, sizeof(char*) * bitmapLen);
		newbitmap[bitmapLen] = (char*)malloc(sizeof(char) * REALTIME_BITMAP_LEN);
		bzero(newbitmap[bitmapLen], REALTIME_BITMAP_LEN);
		gc->addPointer(bitmap);
		bitmap = newbitmap;
		bitmapLen = newBitmapLen;
	}
	time_t t;
	t = time(&t);
	int timeint = (int)t;
	if (timeint - lastGc >= 300) {
		gc->doCollect();
		lastGc = timeint;
	}
	return 1;
}

int Realtime::deleteDoc(int uid) {                                               /* mark delete in incr, full, realtime, if uid exists */
	if (incrDelRef != NULL) {
		char *bitdata = incrDelRef->bitdata;
		int length = incrDelRef->length;
		DocIDMapper *mapper = incrDelRef->mapper;
		if (bitdata != NULL && length > 0 && mapper != NULL) {
			int docid = mapper->getDocID((long)uid);
			if (docid >= 0 && (docid + 8) / 8 <= length) {
				bitdata[(docid / 8)] |= (char)1 << (7 - docid % 8);              /* uid in incrDelRef, set delete mark */
			}
		}
	}
	if (fullDelRef != NULL) {
		char *bitdata = fullDelRef->bitdata;
		int length = fullDelRef->length;
		DocIDMapper *mapper = fullDelRef->mapper;
		if (bitdata != NULL && length > 0 && mapper != NULL) {
			int docid = mapper->getDocID((long)uid);
			if (docid >= 0 && (docid + 8) / 8 <= length) {
				bitdata[(docid / 8)] |= (char)1 << (7 - docid % 8);              /* uid in fullDelRef, set delete mark */
			}
		}
	}
	return doDelete(uid);
}

int Realtime::doBrife() {                                                     /* write brife data of all field into RAMOutputStream */
	int i = 0, j = 0;
	for (i = 0; i < brifeFieldNumber; i++) {
		IndexField *field = brifeField[i];
		int fieldLength = field->getLength();
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		RAMOutputStream *output = brifeOutput[fieldId];                       /* get RAMOutputStream of this field */
		if (data == NULL || length <= 0 || length != fieldLength) {           /* data illegal, write 0 */
			for (j = 0; j < fieldLength; j++) {
				output->writeByte(0);
			}
		} else {
			output->writeBytes(data, 0, length);                              /* write data into RAMOutputStream of this field */
		}
		output->flush();
	}
	return 1;
}

int Realtime::doStore() {                                                      /* write store data into storeLenOutput, storePosOutput, storeDatOutput */
	storeLen = 0;
	int i = 0, number = 0, begin = storeLen++;
	for (i = 0; i < storeFieldNumber; i++) {                                   /* iterate all store field */
		IndexField *field = storeField[i];
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		if (data == NULL || length <= 0) continue;
		number++;
		storeBuffer[storeLen++] = (char)fieldId;                               /* write fieldId into storeBuffer */
		if (field->getLength() == 0) {                                         /* write field length, if length is variable into storeBuffer */
			int vint = length;
			while ((vint & ~0x7F) != 0) {
				storeBuffer[storeLen++] = (char)((vint & 0x7F) | 0x80);
				vint >>= 7;
			}
			storeBuffer[storeLen++] = (char)((vint & 0x7f));
		}
		memcpy(storeBuffer + storeLen, data, length);                          /* write field data into storeBuffer */
		storeLen += length;
	}
	storeBuffer[begin] = (char)number;                                         /* write number of store field in doc into storeBuffer */
	long storeSize = storeDatOutput->getFilePointer();
	writeInt(storeLenOutput, storeLen);                                        /* write store.len */
	writeLong(storePosOutput, storeSize);                                      /* write store.pos */
	storeDatOutput->writeBytes(storeBuffer, 0, storeLen);                      /* write storeBuffer into store.dat */
	storeLenOutput->flush();
	storePosOutput->flush();
	storeDatOutput->flush();
	return 1;
}

int Realtime::doInverted() {                                                    /* analyze index fields, update termDic, postlist and inverted data */
	infoNum = 0;
	dnfterms = NULL;
	conjIds = NULL;
	newIdnum = 0;
	oldIdnum = 0;
	newIds = NULL;
	oldIds = NULL;
	dnftermsNum = 0;
	int i = 0;
	int flag = true;
	for (i = 0; i < indexFieldNumber; i++) {                                     /* analyze index field of doc */
		IndexField *field = indexField[i];
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		int indexType = field->getType();
		if ((data == NULL || length <= 0) && indexType != 2) continue;
		if (indexType == 0) {
			if (textanalyze(data, length, i) <= 0) flag = false;
		} else if (indexType == 1) {
			if (textanalyze(data, length, i) <= 0) flag = false;
		} else if (indexType == 2) {
			if (dnfanalyze(data, length, i) <= 0) flag = false;
		} else if (indexType == 3) {
			if (tagsanalyze(data, length, i) <= 0) flag = false;
		} else {
			return 0;
		}
	}
	if (!flag) return 0;
	addTerms();
	addDnfTerms();
	return 1;
}

/* add terms in infos to termDict, update postlist in positions, write inverted data into termPostPool */
void Realtime::addTerms() {
	if (infoNum <= 0) return;
	int i = 0, j = 0;
	for (i = 0; i < infoNum; i++) {
		TermInfo *info = infos[i];
		int termLen = infoLens[i];
		for (j = 0; j < termLen; j++) {
			TermInfo terminfo = info[j];
			char *payload = terminfo.payload;
			int payloadLen = terminfo.payloadLen;
			int termId = termDict->addTerm(terminfo);
			p = termDict->getPostList(termId);
			assert(p != NULL);
			int invertedStart = p->invertedStart;
			if (invertedStart == -1) {                                                                  /* new term, no inverted data */
				if (BYTE_BLOCK_SIZE - termPostPool->byteUpto < ByteBlockPool::RT_FIRST_LEVEL_SIZE) {
					termPostPool->nextBuffer();
				}
				int upto = termPostPool->newRtSlice(ByteBlockPool::RT_FIRST_LEVEL_SIZE);                /* alloc slice for inverted data */
				int temp = upto + termPostPool->byteOffset;
				p->invertedUpto = upto + termPostPool->byteOffset;
				termVInt(docnum);                                                                       /* write docId into termPostPool */
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {                                             /* write payload into termPostPool */
					termByte(payload[payi]);
				}
				float weight = Utils::encode(payload);
				p->ub = weight;                                                                         /* UB */
				p->invertedStart = temp;
			} else {                                                                                    /* old term, simply add docId and payload to inverted data */
				termVInt(docnum);
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					termByte(payload[payi]);
				}
				float weight = Utils::encode(payload);
				if (weight > p->ub) p->ub = weight;				
			}
		}
	}
}

void Realtime::termVInt(int i) {
	while ((i & ~0x7F) != 0) {
		termByte((char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	termByte((char)i);
}

void Realtime::termByte(char b) {                                            /* write a byte into termPostPool, update p->invertedUpto */
	int upto = p->invertedUpto;
	char *bytes = termPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {
		offset = termPostPool->allocRtSlice(bytes, offset);
		bytes = termPostPool->buffer;
		p->invertedUpto = offset + 1 + termPostPool->byteOffset;
	} else {
		p->invertedUpto = p->invertedUpto + 1;
	}
	bytes[offset] = b;
}

/* add dnf terms in infos to termDict, update postlist in positions, write inverted data into dnftermPostPool, write dnfconjPostIndex and dnfconjPostPool */
void Realtime::addDnfTerms() {
	int i = 0, j = 0;
	if (dnftermsNum > 0) {
		for (j = 0; j < dnftermsNum; j++) {
			TermInfo terminfo = dnfterms[j];
			char *payload = terminfo.payload;
			int payloadLen = terminfo.payloadLen;
			int termId = termDict->addDnfTerm(terminfo);
			dnfp = termDict->getDnfPostList(termId);
			assert(dnfp != NULL);
			int invertedStart = dnfp->invertedStart;
			if (invertedStart == -1) {                                                                      /* new dnf term */
				if (BYTE_BLOCK_SIZE - dnftermPostPool->byteUpto < ByteBlockPool::RT_FIRST_LEVEL_SIZE) {
					dnftermPostPool->nextBuffer();
				}
				int upto = dnftermPostPool->newRtSlice(ByteBlockPool::RT_FIRST_LEVEL_SIZE);                 /* alloc slice for inverted data */
				int temp = upto + dnftermPostPool->byteOffset;
				dnfp->invertedUpto = temp;
				dnftermVInt(conjIds[j]);                                                                    /* write conjId into dnftermPostPool */
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {                                                 /* write belong into dnftermPostPool */
					dnftermByte(payload[payi]);
				}
				dnfp->invertedStart = temp;
			} else {                                                                                        /* old term, simply add inverted data */
				dnftermVInt(conjIds[j]);
				int payi = 0;
				for (payi = 0; payi < payloadLen; payi++) {
					dnftermByte(payload[payi]);
				}
			}
		}
	}
	if (newIdnum > 0) {                                                                                      /* new conj */
		for (i = 0; i < newIdnum; i++) {
			int conjStart = dnfconjPostPool->newRtSlice(ByteBlockPool::RT_FIRST_LEVEL_SIZE);                 /* alloc slice for inverted data */
			if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
				dnfconjPostIndex->nextBuffer();
			}
			dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;  /* inverted start */
			if (dnfconjPostIndex->intUpto == INT_BLOCK_SIZE) {
				dnfconjPostIndex->nextBuffer();
			}
			dnfconjPostIndex->buffer[dnfconjPostIndex->intUpto++] = conjStart + dnfconjPostPool->byteOffset;  /* inverted upto */
			conjVInt(dnfconjPostIndex->intOffset + dnfconjPostIndex->intUpto - 1, docnum);                    /* write docId into dnfconjPostPool */
		}
	}
	if (oldIdnum > 0) {                                                                                       /* old conj, simply add inverted data */
		for (i = 0; i < oldIdnum; i++) {
			int currentConId = oldIds[i];
			int index = currentConId * 2 + 1;
			conjVInt(index, docnum);
		}
	}
}

void Realtime::conjVInt(int off, int i) {
	while ((i & ~0x7F) != 0) {
		conjByte(off, (char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	return conjByte(off, (char)i);
}

void Realtime::conjByte(int off, char b) {                                  /* write a byte into dnfconjPostPool, update dnfconjPostIndex */
	int *data = dnfconjPostIndex->buffers[off >> INT_BLOCK_SHIFT];
	int datoff = off & INT_BLOCK_MASK;
	int upto = data[datoff];
	char *bytes = dnfconjPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {
		offset = dnfconjPostPool->allocRtSlice(bytes, offset);
		bytes = dnfconjPostPool->buffer;
		data[datoff] = offset + 1 + dnfconjPostPool->byteOffset;
	} else {
		data[datoff] = upto + 1;
	}
	bytes[offset] = b;
}

void Realtime::dnftermVInt(int i) {
	while ((i & ~0x7F) != 0) {
		dnftermByte((char)((i & 0x7f) | 0x80));
		i >>= 7;
	}
	dnftermByte((char)i);
}

void Realtime::dnftermByte(char b) {                                                 /* write byte into dnftermPostPool, start from postlist.invertedUpto, then update postlist.invertedUpto */
	int upto = dnfp->invertedUpto;
	char *bytes = dnftermPostPool->buffers[upto >> BYTE_BLOCK_SHIFT];
	int offset = upto & BYTE_BLOCK_MASK;
	if (bytes[offset] != 0) {                                                        /* reach slice level identifier */
		offset = dnftermPostPool->allocRtSlice(bytes, offset);
		bytes = dnftermPostPool->buffer;
		dnfp->invertedUpto = offset + 1 + dnftermPostPool->byteOffset;
	} else {
		dnfp->invertedUpto = dnfp->invertedUpto  + 1;
	}
	bytes[offset] = b;
}

int Realtime::textanalyze(char *data, int length, int fieldId) {                      /* analyze field data, fill TermInfo, add it to infos */
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

int Realtime::tagsanalyze(char *data, int length, int fieldId) {
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

int Realtime::dnfanalyze(char *data, int length, int fieldId) {
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
			if ((newIds[i] * 2) != (base + i * 2)) {                                       /* each conj takes 2 int, newIds[i] should be written in newIds[i] * 2, (newIds[i] * 2)+1 in dnfconjPostIndex */
				return 0;
			}
		}
	}
	int maxsize = dnfAnalyzer->getMaxConjSize();
	if (maxsize > maxconjsize) maxconjsize = maxsize;
	return 1;
}

int Realtime::doDelete(int uid) {                                                          /* if uid in realtime, set delete mark in bitmap */
	map<int, int>::iterator idlist = uidmap.find(uid);
	if (idlist != uidmap.end()) {
		int docid = idlist->second;
		markDeleted(docid);
	}
	return 1;
}

void Realtime::writeInt(RAMOutputStream *output, int value) {                              /* write int into RAMOutputStream */
	output->writeByte((char)(value >> 0));
	output->writeByte((char)(value >> 8));
	output->writeByte((char)(value >> 16));
	output->writeByte((char)(value >> 24));
}

void Realtime::writeVint(RAMOutputStream *output, int value) {
	int vint = value;
	while ((vint & ~0x7F) != 0) {
		output->writeByte((char)((vint & 0x7f) | 0x80));
		vint >>= 7;
	}
	output->writeByte((char)((vint & 0x7f)));
}

void Realtime::writeLong(RAMOutputStream *output, long value) {
	output->writeByte((char)(value >> 0));
	output->writeByte((char)(value >> 8));
	output->writeByte((char)(value >> 16));
	output->writeByte((char)(value >> 24));
	output->writeByte((char)(value >> 32));
	output->writeByte((char)(value >> 40));
	output->writeByte((char)(value >> 48));
	output->writeByte((char)(value >> 56));
}

RAMInputStream* Realtime::brifeSteams(int i) {
	RAMOutputStream *output = brifeOutput[i];
	if (output != NULL) {
		return new RAMInputStream(output->ramfile);
	}
	return NULL;
}

RAMInputStream* Realtime::storeLenStream() {
	return new RAMInputStream(storeLenOutput->ramfile);
}

RAMInputStream* Realtime::storePosStream() {
	return new RAMInputStream(storePosOutput->ramfile);
}

RAMInputStream* Realtime::storeDatStream() {
	return new RAMInputStream(storeDatOutput->ramfile);
}

int Realtime::getDocnum() {
	return docnum;
}

int Realtime::getConjnum() {
	return conjnum;
}

int Realtime::getMaxConjSize() {
	return maxconjsize;
}
