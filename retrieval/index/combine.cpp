#include "indexreader.h"

Combine::Combine(ConjDocs **_docs, int _size) {
	this->docs = _docs;
	this->size = _size;
	this->docid = -1;
	this->begin = 0;
	this->end = 0;
	this->fillFlag = false;
	this->markFlag = 0;
	this->conjListEndNum = 0;
	bzero(mark, BITMAP_BLOCK_SIZE);
	bzero(conjListEnd, MAX_CONJ_SIZE);
	if (!init()) {
		this->isInit = false;
	} else {
		this->isInit = true;
	}
}

Combine::~Combine() {
}

bool Combine::init() {
	bool ret;
	if (NULL == docs) return false;
	for (int i = 0; i < size; i++) {
		if (NULL == docs[i]) return false;
		ret = docs[i]->next();
		if (!ret) {
			conjListEnd[i] = 1;
			conjListEndNum++;
		}
	}
	return true;
}

int Combine::docId() {
	return docid;
}

bool Combine::next() {
	bool ret;
	docid = -1;
	if (!isInit) return false;
	if (!fillFlag) {
		ret = fill();
		if (!ret) {
			docid = INT_MAX;
			return false;
		}
	}
	while (markFlag < BITMAP_BLOCK_SIZE) {
		if (mark[markFlag++] == 1) {
			docid = begin - BITMAP_BLOCK_SIZE + markFlag - 1;
			return true;
		}
	}
	if (conjListEndNum >= size) { 
		docid = INT_MAX;
		return false;
	}
	fillFlag = false;
	return next();
}

int Combine::skipTo(int target) {
	bool ret;
	if (!isInit) return INT_MAX;
	if (docid >= target) return docid;
	if ((target > begin && begin != 0) || (target > (begin + BITMAP_BLOCK_SIZE) && begin == 0)) {
		begin = (target/BITMAP_BLOCK_SIZE) * BITMAP_BLOCK_SIZE;
		fillFlag = false;
		bzero(mark, BITMAP_BLOCK_SIZE);
	}
	if (!fillFlag) {
		ret = fill();
		if (!ret) return INT_MAX;
	}

	if (begin - 1024 < target) markFlag = target%BITMAP_BLOCK_SIZE;
	while (markFlag < BITMAP_BLOCK_SIZE) {
		if (mark[markFlag++] == 1) {
			docid = begin - BITMAP_BLOCK_SIZE + markFlag - 1;
			return docid;
		}
	}
	if (conjListEndNum >= size) return INT_MAX;
	fillFlag = false;
	return skipTo(target);
}

bool Combine::fill() {
	int i;
	int id;
	bool ret;
	bool isfill = false;
	int min = INT_MAX, inmin = INT_MAX; 

	end = begin + BITMAP_BLOCK_SIZE;
	bzero(mark, BITMAP_BLOCK_SIZE);
	if (conjListEndNum >= size) return false;
	for (i = 0; i< size; i++) {
		if (conjListEnd[i] == 1) continue;
		id = docs[i]->docId();
		if (id >= end) {
	//		min = docid < min ? docid : min;
			continue;
		}
		if (id >= begin) {
			mark[id - begin] = 1;
	//		isfill = true;
	//		inmin = (id - begin) < inmin ? (id - begin) : inmin;
		}
		while (ret = docs[i]->next()) {
			id = docs[i]->docId();
			if (id < end) {
				if (id >= begin) {
					mark[id - begin] = 1;
				}
			} else break;
		}
		if (!ret) {
			conjListEndNum ++;
			conjListEnd[i] = 1;
		}
	}

	/*if (!isfill && min < INT_MAX) {
		begin = min;
		fill();
	}else {
		begin += BITMAP_BLOCK_SIZE;
	}*/
	begin += BITMAP_BLOCK_SIZE;

	fillFlag = true;
	//markFlag = inmin;
	markFlag = 0;
	return true;
}

