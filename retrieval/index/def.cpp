#include "def.h"

DocIDMapper::DocIDMapper(long *_uidArray, int uidArrayLen) {      /* make a bloomfilter-like array and data which enables binary search in a little scope for docId */
	MIXER = 2147482951;
	int len = uidArrayLen;
	int _mask = len / 4;
	_mask |= (_mask >> 1);
	_mask |= (_mask >> 2);
	_mask |= (_mask >> 4);
	_mask |= (_mask >> 8);
	_mask |= (_mask >> 16);
	mask = _mask;
	filter = (long*)malloc(sizeof(long) * (mask + 1));
	bzero(filter, sizeof(long) * (mask + 1));
	long uid = 0;
	int i = 0;
	for (i = 0; i < uidArrayLen; i++) {
		uid = _uidArray[i];
		int h = (int)((uid >> 32) ^ uid) * MIXER;
		long bits = filter[h & mask];
		bits |= ((1L << (h >> 26)));
		bits |= ((1L << ((h >> 20) & 0x3F)));
		filter[h & mask] = bits;                                  /* bloomfilter-like array */
	}
	start = (int*)malloc(sizeof(int) * (mask + 2));
	bzero(start, sizeof(int) * (mask + 2));
	len = 0;
	for (i = 0; i < uidArrayLen; i++) {
		uid = _uidArray[i];
		start[((int)((uid >> 32) ^ uid) * MIXER) & mask]++;
		len++;
	}
	int val = 0;
	for (i = 0; i < (mask + 2); i++) {
		val += start[i];
		start[i] = val;
	}
	start[mask] = len;
	long *partitionedUidArray = (long*)malloc(sizeof(long) * len);
	bzero(partitionedUidArray, sizeof(long) * len);
	int *_docArray = (int*)malloc(sizeof(int) * len);
	bzero(_docArray, sizeof(int) * len);
	for (i = 0; i < uidArrayLen; i++) {
		uid = _uidArray[i];
		int j = --(start[((int)((uid >> 32) ^ uid) * MIXER) & mask]);
		partitionedUidArray[j] = uid;
	}
	int s = start[0];
	for (i = 1; i < (mask + 2); i++) {
		int e = start[i];
		if (s < e) {
	        qsort(partitionedUidArray + s, e - s, sizeof(long), DocIDMapper::longCompare);
		}
		s = e;
	}
	for (i = 0; i < uidArrayLen; i++) {
		uid = _uidArray[i];
		int p = ((int)((uid >> 32) ^ uid) * MIXER) & mask;
		int idx = findIndex(partitionedUidArray, uid, start[p], start[p + 1]);
		if (idx >= 0) {
			_docArray[idx] = i;
		}
	}
	uidArray = partitionedUidArray;                                 /* data which enables binary search in a little scope for docId */
	docArray = _docArray;
}

DocIDMapper::~DocIDMapper() {
	if (docArray != NULL) {
		free(docArray);
		docArray = NULL;
	}
	if (uidArray != NULL) {
		free(uidArray);
		uidArray = NULL;
	}
	if (start != NULL) {
		free(start);
		start = NULL;
	}
	if (filter != NULL) {
		free(filter);
		filter = NULL;
	}
}

int DocIDMapper::getDocID(long uid) {                                                                    /* get docId of uid */
	int h = (int)((uid >> 32) ^ uid) * MIXER;
	int p = h & mask;
	long bits = filter[p];
	if ((bits & (1L << (h >> 26))) == 0 || (bits & (1L << ((h >> 20) & 0x3F))) == 0) return -1;          /* like bloom filter  */
	int begin = start[p];
	int end = start[p + 1] - 1;
	while (true) {                                                                                       /* binary search in a little scope */
		int mid = (begin+end) >> 1;
		long midval = uidArray[mid];
		if (midval == uid) return docArray[mid];
		if (mid == end) return -1;
		if (midval < uid) begin = mid + 1;
		else end = mid;
	}
}

int DocIDMapper::findIndex(long *arr, long uid, int begin, int end) {
	if (begin >= end) return -1;
	end--;
	while(true) {
		int mid = (begin+end) >> 1; 
		long midval = arr[mid];
		if (midval == uid) return mid;
		if (mid == end) return -1;
		if (midval < uid) begin = mid + 1;
		else end = mid;
	}
}

int DocIDMapper::longCompare(const void *a, const void *b) {
	return *(long*)a - *(long*)b;
}

SimpleGc::SimpleGc() {                                                      /* auto delete pointers added */
	size = 10240;
	used = 0;
	toFree = (void**)malloc(sizeof(void*) * size);
	inTime = (int*)malloc(sizeof(int) * size);
	bzero(toFree, sizeof(void*) * size);
	bzero(inTime, sizeof(int) * size);
}

SimpleGc::~SimpleGc() {
	int i = 0;
	if (toFree != NULL) {
		for (i = 0; i < used; i++) {
			void *pointer = toFree[i];
			if (pointer != NULL) free(pointer);
			toFree[i] = NULL;
		}
		free(toFree);
		toFree = NULL;
	}
	if (inTime != NULL) {
		free(inTime);
		inTime = NULL;
	}
}

void SimpleGc::addPointer(void *pointer) {
	time_t t;
	t = time(&t);
	int timeint = (int)t;
	if (used == size) {
		if ((timeint - 600) > inTime[0]) sleep(timeint - 600 - inTime[0]);
		doCollect();
	}
	toFree[used] = pointer;
	inTime[used] = timeint;
	used++;
}

void SimpleGc::doCollect() {
	time_t t;
	t = time(&t);
	int timeint = (int)t, i = 0, num = 0;
	timeint -= 600;
	for (i = 0; i < used; i++) {
		if (inTime[i] > timeint) break;                                     /* only delete pointers added 600 seconds ago */
		else {
			if (toFree[i] != NULL) {
				free(toFree[i]);
				toFree[i] = NULL;
				inTime[i] = 0;
			}
			num++;
		}
	}
	if (num > 0) {
		if (used - num > 0) {
		    memcpy(toFree, toFree + num, sizeof(void*) * (used - num));
		    memcpy(inTime, inTime + num, sizeof(int) * (used - num));
		}
		used -= num;
	}
}
