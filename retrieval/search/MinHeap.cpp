#include "MinHeap.h"

MinHeap::MinHeap(const size_t maxSize) {
	_size = 0;
	this->maxSize = maxSize;
	heap = new FieldDoc[maxSize+1];
	minScore = -1.0f;
}

MinHeap::~MinHeap() {
	delete [] heap;
}

float MinHeap::getMinScore() {
	if(_size < maxSize) return 0.0;
	return heap[1].score;
}

SimpleMinHeap::SimpleMinHeap(const size_t maxSize) : MinHeap(maxSize) {
	_size = 0;
	this->maxSize = maxSize;
	heap = new FieldDoc[maxSize+1];
	minScore = -1.0f;
}

SimpleMinHeap::~SimpleMinHeap() {
	delete [] heap;
}

FieldDoc& SimpleMinHeap::top() {
	return heap[1];
}

float SimpleMinHeap::getMinScore() {
	if(_size < maxSize) return 0.0;
	return heap[1].score;
}


void SimpleMinHeap::upHeap() {
	size_t i = _size;
	FieldDoc node = heap[i];			  // save bottom node (WAS object)
	int32_t j = ((uint32_t)i) >> 1;
	while (j > 0 && lessThan(node,heap[j])) {
		heap[i] = heap[j];			  // shift parents down
		i = j;
		j = ((uint32_t)j) >> 1;
	}
	heap[i] = node;				  // install saved node
}

void SimpleMinHeap::downHeap() {
	size_t i = 1;
	FieldDoc node = heap[i];			  // save top node
	size_t j = i << 1;				  // find smaller child
	size_t k = j + 1;
	if (k <= _size && lessThan(heap[k], heap[j])) {
		j = k;
	}
	while (j <= _size && lessThan(heap[j],node)) {
		heap[i] = heap[j];			  // shift up child
		i = j;
		j = i << 1;
		k = j + 1;
		if (k <= _size && lessThan(heap[k], heap[j])) {
			j = k;
		}
	}
	heap[i] = node;				  // install saved node
}

bool SimpleMinHeap::ranbool() {
	return rand()%2;
}


void SimpleMinHeap::put(FieldDoc& element){                      /* put a element into heap, and sort the heap */
	_size++;	
	heap[_size] = element;		
	upHeap();
}

FieldDoc SimpleMinHeap::pop(){                                   /* pop min element of the heap, and sort the heap */
	if (_size > 0) {
		FieldDoc result = heap[1];			  // save first value
		heap[1] = heap[_size];			  // move last to first
		_size--;
		downHeap();				  // adjust heap
		return result;
	} else {
		FieldDoc result;
		return result;
	}
}

bool SimpleMinHeap::insert(FieldDoc& element){                    /* try to insert a element into minHeap */
	if (_size < maxSize) {
		put(element);
		return true;
	} else if(_size > 0 && !lessThan(element, heap[1])) {         /* element bigger than top of heap, insert */
		heap[1] = element;
		downHeap();
		return true;
	} else {
		return false;
	}
}

bool SimpleMinHeap::lessThan(FieldDoc& hitA, FieldDoc& hitB) {
	if (hitA.score == hitB.score) {
		return this->ranbool(); 
	} else {
		return hitA.score < hitB.score;
	}
}


SortedIRanMinHeap::SortedIRanMinHeap(const size_t maxSize):
	MinHeap(maxSize) {
	_size = 0;
	this->maxSize = maxSize;
	heap = new FieldDoc[maxSize+1];
	minScore = -1.0f;
	srand(time(0));
}

SortedIRanMinHeap::~SortedIRanMinHeap() {
	delete [] heap;
}

FieldDoc& SortedIRanMinHeap::top() {
	return heap[1];
}

float SortedIRanMinHeap::getMinScore() {
	if(_size < maxSize) return 0.0;
	return 0.0;
}

void SortedIRanMinHeap::upHeap() {
	size_t i = _size;
	FieldDoc node = heap[i];			  // save bottom node (WAS object)
	int32_t j = ((uint32_t)i) >> 1;
	while (j > 0 && lessThan(node,heap[j])) {
		heap[i] = heap[j];			  // shift parents down
		i = j;
		j = ((uint32_t)j) >> 1;
	}
	heap[i] = node;				  // install saved node
}
void SortedIRanMinHeap::downHeap() {
	size_t i = 1;
	FieldDoc node = heap[i];			  // save top node
	size_t j = i << 1;				  // find smaller child
	size_t k = j + 1;
	if (k <= _size && lessThan(heap[k], heap[j])) {
		j = k;
	}
	while (j <= _size && lessThan(heap[j],node)) {
		heap[i] = heap[j];			  // shift up child
		i = j;
		j = i << 1;
		k = j + 1;
		if (k <= _size && lessThan(heap[k], heap[j])) {
			j = k;
		}
	}
	heap[i] = node;				  // install saved node
}

bool SortedIRanMinHeap::ranbool() {
	return rand()%2;
}

void SortedIRanMinHeap::put(FieldDoc& element) {
	_size++;	
	heap[_size] = element;		
	upHeap();
}

FieldDoc SortedIRanMinHeap::pop() {
	if (_size > 0) {
		FieldDoc result = heap[1];			  // save first value
		heap[1] = heap[_size];			  // move last to first
		_size--;
		downHeap();				  // adjust heap
		return result;
	} else {
		FieldDoc result;
		return result;
	}
}

bool SortedIRanMinHeap::insert(FieldDoc& element) {
	if(_size < maxSize) {
		put(element);
		return true;
	} else if (_size > 0 && !lessThan(element, heap[1])) {
		heap[1] = element;
		downHeap();
		return true;
	} else
		return false;
}

#if 0
bool SortedIRanMinHeap::lessThan(FieldDoc& hitA, FieldDoc& hitB) {
	if (hitA.fields == hitB.fields) {
		return this->ranbool();
	} else {
		return hitA.fields < hitB.fields;
	}
}
#endif

bool SortedIRanMinHeap::lessThan(FieldDoc& hitA, FieldDoc& hitB) {
    if (hitA.fields == hitB.fields) {
        if (hitA.random == hitB.random) {
            return this->ranbool();
    	} else {
            return hitA.random < hitB.random;
        }
    } else {
        return hitA.fields < hitB.fields;
    }
}

int SortedIRanMinHeap::ranint() {
    //return rand() % this->maxSize;
    return rand() % 1000 /* 满足条件的最大doc数 */ ;
}
