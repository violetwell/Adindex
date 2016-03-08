#include "pool.h"

Allocator::Allocator() {
	blockSize = BYTE_BLOCK_SIZE;
	freeInt = 0;
	freeByte = 0;
	intSize = 10;
	byteSize = 10;
	freeIntBlocks = (int**)malloc(sizeof(int*) * 10);
	freeByteBlocks = (char**)malloc(sizeof(char*) * 10);
	postingsFreeList = NULL;
	postingsFreeCount = 0;
	postingsAllocCount = 0;
	dnfpostingsFreeList = NULL;
	dnfpostingsFreeCount = 0;
	dnfpostingsAllocCount = 0;
}

Allocator::~Allocator() {
	int i = 0;
	if (freeIntBlocks != NULL) {
		for (i = 0; i < freeInt; i++) {
			int *block = freeIntBlocks[i];
			if (block != NULL) free(block);
		}
		free(freeIntBlocks);
	}
	if (freeByteBlocks != NULL) {
		for (i = 0; i < freeByte; i++) {
			char *block = freeByteBlocks[i];
			if (block != NULL) free(block);
		}
		free(freeByteBlocks);
	}
	if (postingsFreeList != NULL) {
		for (i = 0; i < postingsFreeCount; i++) {
			RawPostingList *block = postingsFreeList[i];
			if (block != NULL) free(block);
		}
		free(postingsFreeList);
	}
	if (dnfpostingsFreeList != NULL) {
		for (i = 0; i < dnfpostingsFreeCount; i++) {
			DnfRawPostingList *block = dnfpostingsFreeList[i];
			if (block != NULL) free(block);
		}
		free(dnfpostingsFreeList);
	}
}

int Allocator::getBlockSize() {
	return blockSize;
}

char* Allocator::getByteBlock() {                                                            /* return a byteBlock */
	char *block = NULL;
    if (freeByte == 0) {                                                                     /* no recycled byteBlock */
		block = (char*)malloc(sizeof(char) * blockSize);
		bzero(block, sizeof(char) * blockSize);
	} else {                                                                                 /* return a recycled byteBlock */
		block = freeByteBlocks[--freeByte];
		bzero(block, sizeof(char) * blockSize);
		freeByteBlocks[freeByte] = NULL;
	}
    return block;
}

void Allocator::recycleByteBlocks(char **blocks, int start, int end) {                       /* recycled byteBlock */
	int size = end - start;
	if (size <= 0) return;
	if ((byteSize - freeByte) < size) {
		char **temp = freeByteBlocks;
		char **newfreeByteBlocks = (char**)malloc(sizeof(char*) * (size + freeByte));
		memcpy(newfreeByteBlocks, freeByteBlocks, freeByte * sizeof(char*));
		freeByteBlocks = newfreeByteBlocks;
		free(temp);
		byteSize = freeByte + size;
	}
	memcpy(freeByteBlocks + freeByte, blocks + start, size * sizeof(char*));
	freeByte += size;
}

int* Allocator::getIntBlock() {
	int *block = NULL;
    if (freeInt == 0) {
		block = (int*)malloc(sizeof(int) * INT_BLOCK_SIZE);
		bzero(block, sizeof(int) * INT_BLOCK_SIZE);
	} else {
		block = freeIntBlocks[--freeInt];
		bzero(block, sizeof(int) * INT_BLOCK_SIZE);
		freeIntBlocks[freeInt] = NULL;
	}
    return block;
}

void Allocator::recycleIntBlocks(int **blocks, int start, int end) {
	int size = end - start;
	if (size <= 0) return;
	if ((intSize - freeInt) < size) {
		int **temp = freeIntBlocks;
		int **newfreeIntBlocks = (int**)malloc(sizeof(int*) * (size + freeInt));
		memcpy(newfreeIntBlocks, freeIntBlocks, freeInt * sizeof(int*));
		freeIntBlocks = newfreeIntBlocks;
		free(temp);
		intSize = freeInt + size;
	}
	memcpy(freeIntBlocks + freeInt, blocks + start, size * sizeof(int*));
	freeInt += size;
}

void Allocator::getPostings(RawPostingList **postings, int number) {
	int numToCopy = 0;
	if (postingsFreeCount < number) numToCopy = postingsFreeCount;
	else numToCopy = number;
	int start = postingsFreeCount - numToCopy;
	if (numToCopy > 0) {
		memcpy(postings, postingsFreeList + start, sizeof(RawPostingList*) * numToCopy);
	}
	if (numToCopy != number) {
		int extra = number - numToCopy;
		int newPostingsAllocCount = postingsAllocCount + extra;
		createPostings(postings, numToCopy, extra);
		postingsAllocCount += extra;
		RawPostingList** older = postingsFreeList;
		postingsFreeList = (RawPostingList**)malloc(sizeof(RawPostingList*) 
		* getNextSize(newPostingsAllocCount));                                                             /* expand list */
		if (older != NULL) free(older);
	}
	postingsFreeCount -= numToCopy;
}

void Allocator::recyclePostings(RawPostingList **postings, int numPostings) {
	assert(postingsAllocCount >= postingsFreeCount + numPostings);
	memcpy(postingsFreeList + postingsFreeCount, postings, sizeof(RawPostingList*) * numPostings);
	postingsFreeCount += numPostings;
}

void Allocator::createPostings(RawPostingList **postings, int start, int count) {
	int end = start + count, i = 0;
	for (i = start; i < end; i++) {
		postings[i] = (RawPostingList*)malloc(sizeof(RawPostingList));
	}
}

void Allocator::getDnfPostings(DnfRawPostingList **postings, int number) {
	int numToCopy = 0;
	if (dnfpostingsFreeCount < number) numToCopy = dnfpostingsFreeCount;
	else numToCopy = number;
	int start = dnfpostingsFreeCount - numToCopy;
	if (numToCopy > 0) {
		memcpy(postings, dnfpostingsFreeList + start, sizeof(DnfRawPostingList*) * numToCopy);
	}
	if (numToCopy != number) {
		int extra = number - numToCopy;
		int newPostingsAllocCount = dnfpostingsAllocCount + extra;
		createDnfPostings(postings, numToCopy, extra);
		dnfpostingsAllocCount += extra;
		DnfRawPostingList** older = dnfpostingsFreeList;
		dnfpostingsFreeList = (DnfRawPostingList**)malloc(sizeof(DnfRawPostingList*) 
		* getNextSize(newPostingsAllocCount));
		if (older != NULL) free(older);
	}
	dnfpostingsFreeCount -= numToCopy;
}

int Allocator::getNextSize(int targetSize) {
	return (targetSize >> 3) + (targetSize < 9 ? 3 : 6) + targetSize;
}

void Allocator::recycleDnfPostings(DnfRawPostingList **postings, int numPostings) {
	assert(dnfpostingsAllocCount >= dnfpostingsFreeCount + numPostings);
	memcpy(dnfpostingsFreeList + dnfpostingsFreeCount, postings, sizeof(DnfRawPostingList*) * numPostings);
	dnfpostingsFreeCount += numPostings;
}

void Allocator::createDnfPostings(DnfRawPostingList **postings, int start, int count) {
	int end = start + count, i = 0;
	for (i = start; i < end; i++) {
		postings[i] = (DnfRawPostingList*)malloc(sizeof(DnfRawPostingList));
	}
}

IntBlockPool::IntBlockPool(Allocator *_allocator, SimpleGc *_gc) {
	allocator = _allocator;
	buffers = (int**)malloc(sizeof(int*) * 10);
	bufferUpto = -1;
	intUpto = INT_BLOCK_SIZE;
	buffer = NULL;
	intOffset = -INT_BLOCK_SIZE;
	bufferSize = 10;
	gc = _gc;
}

IntBlockPool::IntBlockPool(Allocator *_allocator) {
	allocator = _allocator;
	buffers = (int**)malloc(sizeof(int*) * 10);
	bufferUpto = -1;
	intUpto = INT_BLOCK_SIZE;
	buffer = NULL;
	intOffset = -INT_BLOCK_SIZE;
	bufferSize = 10;
}

IntBlockPool::~IntBlockPool() {
	int i = 0;
	for (i = 0; i <= bufferUpto; i++) {
		int *data = buffers[i];
		if (data != NULL) {
			free(data);
		}
	}
	free(buffers);
}

void IntBlockPool::reset() {
	if (bufferUpto != -1) {
		if (bufferUpto > 0) allocator->recycleIntBlocks(buffers, 1, 1 + bufferUpto);
		bufferUpto = 0;
		intUpto = 0;
		intOffset = 0;
		buffer = buffers[0];
	}
}

void IntBlockPool::nextBuffer() {
	if (1 + bufferUpto == bufferSize) {
		int **temp = buffers;
		int **newBuffers = (int**)malloc(sizeof(int*) * bufferSize * 2);
		memcpy(newBuffers, buffers, sizeof(int*) * bufferSize);
		buffers = newBuffers;
		bufferSize = bufferSize * 2;
		if (gc == NULL) free(temp);
		else gc->addPointer(temp);
	}
	buffer = buffers[1 + bufferUpto] = allocator->getIntBlock();
	bufferUpto++;
	intUpto = 0;
	intOffset += INT_BLOCK_SIZE;
}

int ByteBlockPool::nextLevelArray[10] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 9 };
int ByteBlockPool::levelSizeArray[10] = { 5, 14, 20, 30, 40, 40, 80, 80, 120, 200 };
int ByteBlockPool::FIRST_LEVEL_SIZE = 5;

int ByteBlockPool::nextRtLevelArray[6] = { 1, 2, 3, 4, 5, 5 };
int ByteBlockPool::levelRtSizeArray[6] = { 16, 128, 256, 512, 1024, 1024 };
int ByteBlockPool::RT_FIRST_LEVEL_SIZE = 16;

ByteBlockPool::ByteBlockPool(Allocator *_allocator, SimpleGc *_gc) {
	allocator = _allocator;
	buffers = (char**)malloc(sizeof(char*) * 10);
	bufferSize = 10;
	bufferUpto = -1;
	byteUpto = BYTE_BLOCK_SIZE;
	buffer = NULL;
	byteOffset = - BYTE_BLOCK_SIZE;
	gc = _gc;
}

ByteBlockPool::ByteBlockPool(Allocator *_allocator) {
	allocator = _allocator;
	buffers = (char**)malloc(sizeof(char*) * 10);
	bufferSize = 10;
	bufferUpto = -1;
	byteUpto = BYTE_BLOCK_SIZE;
	buffer = NULL;
	byteOffset = - BYTE_BLOCK_SIZE;
}

ByteBlockPool::~ByteBlockPool() {
	int i = 0;
	for (i = 0; i <= bufferUpto; i++) {
		char *data = buffers[i];
		if (data != NULL) {
			free(data);
		}
	}
	free(buffers);
}

void ByteBlockPool::reset() {
	if (bufferUpto != -1) {
		int i = 0;
		for (; i < bufferUpto; i++) {
			bzero(buffers[i], allocator->getBlockSize());
		}
		bzero(buffers[bufferUpto], byteUpto);
		if (bufferUpto > 0) allocator->recycleByteBlocks(buffers, 1, 1 + bufferUpto);  /* recycle buffers, leave buffers[0] */
		bufferUpto = 0;
		byteUpto = 0;
		byteOffset = 0;
		buffer = buffers[0];
	}
}

void ByteBlockPool::nextBuffer() {                                                  /* alloc a new buffer from allocator */
	if (1 + bufferUpto == bufferSize) {                                             /* expand buffers */
		char **temp = buffers;
		char **newBuffers = (char**)malloc(sizeof(int*) * bufferSize * 2);
		memcpy(newBuffers, buffers, sizeof(char*) * bufferSize);
		buffers = newBuffers;
		bufferSize = bufferSize * 2;
		if (gc == NULL) free(temp);
		else gc->addPointer(temp);
	}
	buffer = buffers[1 + bufferUpto] = allocator->getByteBlock();
	bufferUpto++;
	byteUpto = 0;
	byteOffset += BYTE_BLOCK_SIZE;
}

int ByteBlockPool::newSlice(int size) {
	if (byteUpto > BYTE_BLOCK_SIZE - size) nextBuffer();                            /* not enough room in current buffer */
	int upto = byteUpto;
	byteUpto += size;
	buffer[byteUpto - 1] = 16;                                                      /* identifier of level, 16&15 = 0 */
	return upto;
}

int ByteBlockPool::newRtSlice(int size) {
	if (byteUpto > BYTE_BLOCK_SIZE - size) nextBuffer();
	int upto = byteUpto;
	byteUpto += size;
	buffer[byteUpto - 4] = 16;                                                      /* identifier of level, located 3 bytes away from end */
	return upto;
}

int ByteBlockPool::allocRtSlice(char *slice, int upto) {                          /* alloc a new slice, return the position to write */
	int level = slice[upto] & 15;
	int newLevel = ByteBlockPool::nextRtLevelArray[level];
	int newSize = ByteBlockPool::levelRtSizeArray[newLevel];
	if (byteUpto > BYTE_BLOCK_SIZE - newSize) nextBuffer();
	int newUpto = byteUpto;
	int offset = newUpto + byteOffset;
	byteUpto += newSize;
	slice[upto] = (char)(offset >> 24);                                           /* write pointer, last 3 bytes not used, so no need to copy data from old to new slice */
	slice[upto + 1] = (char)(offset >> 16);
	slice[upto + 2] = (char)(offset >> 8);
	slice[upto + 3] = (char)offset;
	buffer[byteUpto - 4] = (char)(16 | newLevel);                                 /* identifier of level */
	return newUpto;
}

int ByteBlockPool::allocSlice(char *slice, int upto) {                            /* alloc a new slice, return the position to write */
	int level = slice[upto] & 15;
	int newLevel = ByteBlockPool::nextLevelArray[level];
	int newSize = ByteBlockPool::levelSizeArray[newLevel];
	if (byteUpto >= BYTE_BLOCK_SIZE - newSize) nextBuffer();
	int newUpto = byteUpto;
	int offset = newUpto + byteOffset;
	byteUpto += newSize;
	buffer[newUpto] = slice[upto - 3];                                            /* copy last 3 bytes to new slice */
	buffer[newUpto + 1] = slice[upto - 2];
	buffer[newUpto + 2] = slice[upto - 1];
	slice[upto - 3] = (char)(offset >> 24);                                       /* write a pointer in end of old slice, which points to start of new slice */
	slice[upto - 2] = (char)(offset >> 16);
	slice[upto - 1] = (char)(offset >> 8);
	slice[upto] = (char)offset;
	buffer[byteUpto - 1] = (char)(16 | newLevel);                                 /* identifier of level */
	return newUpto + 3;
}

ByteSliceReader::ByteSliceReader() {
}

ByteSliceReader::~ByteSliceReader() {
}

void ByteSliceReader::init(ByteBlockPool *_pool, int _startIndex, int _endIndex) {
	assert(_endIndex - _startIndex >= 0);
	assert(_startIndex >= 0);
	assert(_endIndex >= 0);
	pool = _pool;
	endIndex = _endIndex;
	level = 0;
	bufferUpto = _startIndex / BYTE_BLOCK_SIZE;
	bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
	buffer = pool->buffers[bufferUpto];
	upto = _startIndex & BYTE_BLOCK_MASK;
	int firstSize = ByteBlockPool::levelSizeArray[0];
	if (_startIndex + firstSize >= _endIndex) {                    /* _startIndex and _endIndex point to the same slice, ps: slice pointer */
		limit = _endIndex & BYTE_BLOCK_MASK;
	} else {
		limit = upto + firstSize - 4;                              /* reach limit means reach the slice pointer */
	}
}

bool ByteSliceReader::eof() {
	assert(upto + bufferOffset <= endIndex);
	return upto + bufferOffset == endIndex;
}

char ByteSliceReader::readByte() {
	assert(!eof());
	assert(upto <= limit);
	if (upto == limit) nextSlice();
	return buffer[upto++];
}

void ByteSliceReader::nextSlice() {
	int nextIndex = ((buffer[limit] & 0xff) << 24) + ((buffer[1 + limit] & 0xff) << 16)
	+ ((buffer[2 + limit] & 0xff) << 8) + (buffer[3 + limit] & 0xff);
	level = ByteBlockPool::nextLevelArray[level];
	int newSize = ByteBlockPool::levelSizeArray[level];
	bufferUpto = nextIndex / BYTE_BLOCK_SIZE;
	bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
	buffer = pool->buffers[bufferUpto];
	upto = nextIndex & BYTE_BLOCK_MASK;
	if (nextIndex + newSize >= endIndex) {
		assert(endIndex - nextIndex > 0);
		limit = endIndex - bufferOffset;
	} else {
		limit = upto + newSize - 4;
	}
}

void ByteSliceReader::readBytes(char *b, int offset, int len) {
	while (len > 0) {
		int numLeft = limit - upto;
		if (numLeft < len) {
			memcpy(b + offset, buffer + upto, numLeft);
			offset += numLeft;
			len -= numLeft;
			nextSlice();
		} else {
			memcpy(b + offset, buffer + upto, len);
			upto += len;
			break;
		}
	}
}

RtByteSliceReader::RtByteSliceReader() {        /* the same as ByteSliceReader, only difference: realtime don't write data in last 3 bytes of slice */
}

RtByteSliceReader::~RtByteSliceReader() {
}

void RtByteSliceReader::init(ByteBlockPool *_pool, int _startIndex, int _endIndex) {
	assert(_endIndex - _startIndex >= 0);
	assert(_startIndex >= 0);
	assert(_endIndex >= 0);
	pool = _pool;
	endIndex = _endIndex;
	level = 0;
	bufferUpto = _startIndex / BYTE_BLOCK_SIZE;
	bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
	buffer = pool->buffers[bufferUpto];
	upto = _startIndex & BYTE_BLOCK_MASK;
	int firstSize = ByteBlockPool::levelRtSizeArray[0];
	if (_startIndex + firstSize - 4 >= _endIndex) {
		limit = _endIndex & BYTE_BLOCK_MASK;
	} else limit = upto + firstSize - 4;
}

bool RtByteSliceReader::eof() {
	assert(upto + bufferOffset <= endIndex);
	return upto + bufferOffset == endIndex;
}

char RtByteSliceReader::readByte() {
	assert(!eof());
	assert(upto <= limit);
	if (upto == limit) nextSlice();
	return buffer[upto++];
}

void RtByteSliceReader::nextSlice() {
	int nextIndex = ((buffer[limit] & 0xff) << 24) + ((buffer[1 + limit] & 0xff) << 16)
	+ ((buffer[2 + limit] & 0xff) << 8) + (buffer[3 + limit] & 0xff);
	level = ByteBlockPool::nextRtLevelArray[level];
	int newSize = ByteBlockPool::levelRtSizeArray[level];
	bufferUpto = nextIndex / BYTE_BLOCK_SIZE;
	bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
	buffer = pool->buffers[bufferUpto];
	upto = nextIndex & BYTE_BLOCK_MASK;
	if (nextIndex + newSize - 4 >= endIndex) {
		assert(endIndex - nextIndex > 0);
		limit = endIndex - bufferOffset;
	} else {
		limit = upto + newSize - 4;
	}
}

void RtByteSliceReader::readBytes(char *b, int offset, int len) {
	while (len > 0) {
		int numLeft = limit - upto;
		if (numLeft < len) {
			memcpy(b + offset, buffer + upto, numLeft);
			offset += numLeft;
			len -= numLeft;
			nextSlice();
		} else {
			memcpy(b + offset, buffer + upto, len);
			upto += len;
			break;
		}
	}
}
