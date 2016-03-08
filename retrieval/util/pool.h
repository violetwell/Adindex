#ifndef  __POOL_H_
#define  __POOL_H_

#include <string>
#include <assert.h>
#include "def.h"

class Allocator {
	
public:
	Allocator();
	~Allocator();
	char* getByteBlock();
	void recycleByteBlocks(char **blocks, int start, int end);
	int* getIntBlock();
	void recycleIntBlocks(int **blocks, int start, int end);
	void getPostings(RawPostingList **postings, int number);
	void recyclePostings(RawPostingList **postings, int numPostings);
	void getDnfPostings(DnfRawPostingList **postings, int number);
	void recycleDnfPostings(DnfRawPostingList **postings, int numPostings);
	int getBlockSize();
	
private:
	int blockSize;
	
	int freeInt;
	int intSize;
	int **freeIntBlocks;
	
	int freeByte;
	int byteSize;
	char **freeByteBlocks;
	
	RawPostingList **postingsFreeList;
	int postingsFreeCount;
	int postingsAllocCount;
	void createPostings(RawPostingList **postings, int start, int count);
	
	DnfRawPostingList **dnfpostingsFreeList;
	int dnfpostingsFreeCount;
	int dnfpostingsAllocCount;
	void createDnfPostings(DnfRawPostingList **postings, int start, int count);
	int getNextSize(int targetSize);
	
};

class IntBlockPool {
	
public:
	IntBlockPool(Allocator *_allocator);
	IntBlockPool(Allocator *_allocator, SimpleGc *_gc);
	~IntBlockPool();
	void reset();
	void nextBuffer();
	
	int **buffers;
	int bufferUpto;
	int intUpto;
	int *buffer;
	int intOffset;
	
private:
	int bufferSize;
	Allocator *allocator;
	SimpleGc *gc;
	
};

class ByteBlockPool {
	
public:
	ByteBlockPool(Allocator *_allocator);
	ByteBlockPool(Allocator *_allocator, SimpleGc *_gc);
	~ByteBlockPool();
	void reset();
	void nextBuffer();
	int newSlice(int size);
	int allocSlice(char *slice, int upto);
	int newRtSlice(int size);
	int allocRtSlice(char *slice, int upto);
	
	char **buffers;                                        /* array of pointers, which will be used to point to buffer */
	int bufferUpto;                                        /* index of the buffer in use */
	int byteUpto;                                          /* index of the unused byte in buffer */
	char *buffer;                                          /* buffer in use */
	int byteOffset;                                        /* blocksize * bufferUpto */
	
private:
	int bufferSize;                                        /* size of buffers */
	Allocator *allocator;
	SimpleGc *gc;
	
public:
	static int nextLevelArray[10];
	static int levelSizeArray[10];
	static int FIRST_LEVEL_SIZE;
	static int nextRtLevelArray[6];
	static int levelRtSizeArray[6];
	static int RT_FIRST_LEVEL_SIZE;
	
};

class ByteSliceReader {
	
public:
	ByteSliceReader();
	~ByteSliceReader();
	void init(ByteBlockPool *pool, int startIndex, int endIndex);
	bool eof();
	char readByte();
	void nextSlice();
	void readBytes(char *b, int offset, int len);
	
private:
	int bufferUpto;
	char* buffer;
	int upto;
	int limit;
	int level;
	int bufferOffset;
	int endIndex;
	ByteBlockPool *pool;
	
};

class RtByteSliceReader {
	
public:
	RtByteSliceReader();
	~RtByteSliceReader();
	void init(ByteBlockPool *pool, int startIndex, int endIndex);
	bool eof();
	char readByte();
	void nextSlice();
	void readBytes(char *b, int offset, int len);
	
private:
	int bufferUpto;
	char* buffer;
	int upto;
	int limit;
	int level;
	int bufferOffset;
	int endIndex;
	ByteBlockPool *pool;
	
};

#endif
