#ifndef MINHEAP_H
#define MINHEAP_H

#include "SearchHeader.h"
#include "stdint.h"
#include "stdlib.h"
#include <string>
#include <string.h>
#include "time.h"


class MinHeap {
private:
	FieldDoc* heap;
	size_t _size;
	size_t maxSize;
	float minScore;

	virtual void upHeap() = 0;
	virtual void downHeap() = 0;

protected:
	virtual bool lessThan(FieldDoc& hitA, FieldDoc& hitB) = 0;

public:
	int isFull()const;
	size_t size()const;
	float getMinScore();
	virtual FieldDoc& top() = 0;

	virtual void put(FieldDoc& element) = 0;
	virtual FieldDoc pop() = 0;
	virtual bool insert(FieldDoc& element) = 0;

	MinHeap(const size_t maxSize);
	~MinHeap();
};

inline int MinHeap::isFull()const {
	return _size==maxSize;
}

inline size_t MinHeap::size()const {
	return _size;
}

/* minHeap which compares score of FieldDoc */
class SimpleMinHeap:public MinHeap {
private:
	FieldDoc* heap;
	size_t _size;
	size_t maxSize;                                       /* count of top docs needed */
	float minScore;

	void upHeap();
	void downHeap();

	bool ranbool();
protected:
	bool lessThan(FieldDoc& hitA, FieldDoc& hitB);

public:
	int isFull()const;
	size_t size()const;
	float getMinScore();

	FieldDoc& top();

	void put(FieldDoc& element);
	FieldDoc pop();
	bool insert(FieldDoc& element);

	SimpleMinHeap(const size_t maxSize);
	~SimpleMinHeap();
};

inline int SimpleMinHeap::isFull()const {
	return _size==maxSize;
}

inline size_t SimpleMinHeap::size()const {
	return _size;
}

/* minHeap which compares fields of FieldDoc */
class SortedIRanMinHeap:public MinHeap {
private:
	FieldDoc* heap;
	size_t _size;
	size_t maxSize;
	float minScore;

	void upHeap();
	void downHeap();

	bool ranbool();

protected:
	bool lessThan(FieldDoc& hitA, FieldDoc& hitB);

public:
	int isFull()const;
	size_t size()const;
	float getMinScore();

	FieldDoc& top();

	void put(FieldDoc& element);
	FieldDoc pop();
	bool insert(FieldDoc& element);

	SortedIRanMinHeap(const size_t maxSize);
	~SortedIRanMinHeap();

public:
    int ranint();
};

inline int SortedIRanMinHeap::isFull()const {
	return _size==maxSize;
}

inline size_t SortedIRanMinHeap::size()const {
	return _size;
}

#endif
