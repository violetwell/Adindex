#include "filter.h"

Filter::Filter(){
}
Filter::~Filter(){
}

/* _brife: brife data of _fid of all docs, _len:field length, _content:data of filter query */
DiskBrifeFilter::DiskBrifeFilter(unsigned char * _brife, int _len, int _fid, char * _content) {
	brifeData = _brife;
	len = _len;
	fid = _fid;
	content = _content;
	if (NULL == brifeData || len <= 0 || NULL == content) isInit = false;
	else isInit = true;
}

DiskBrifeFilter::~DiskBrifeFilter() {
}

bool DiskBrifeFilter::filterData(int id) {
	if (!isInit) return false;

	//	printf("DiskBrifeFilter::filterData id = %d\n", id);
	int pos = id * len;
	unsigned char * brifestart = brifeData + pos;

	if (fid == CONT_TIME_FIELD) {                                                 /* TODO: don't understand */
		if (len != 4) return false;
		int data = *((int *)content);
		int indexdata = *((int *)(brifestart));
		if (indexdata < 0 ) return false;
		if (indexdata >= data) return true;
		else return false;
	}

	if (fid == PERIODS_FIELD) {                                                   /* filter period field */
		if (len%PERIODS_BYTE_NUM != 0) return false;
		long long data = *((long long *)content);
		long long data1 = *((long long*)brifestart);
		long long data2 = *((long long *)(brifestart + PERIODS_BYTE_NUM/2));
		//printf("DiskBrifeFilter::filterData  data = %lld, data1 = %lld, data2 = %lld\n", data, data1,data2);
		if (data1 < 0 || data2 < 0) return false;
		if (data1 == 0 && data2 == 0) return true;
		if (data >= data1 && data <= data2) {                                     /* time of filter query is in time region [data1, data2] */
			//printf("DiskBrifeFilter::filterData return true\n");
			return true;
		}
		else return false;
	}
}

/* _filterInfo: storeFilterInfo, size: schema size */
DiskStoreFilter::DiskStoreFilter(Handler * _handle, FilterInfo * _filterInfo, int * _fieldlen, int _size) {
	handle = NULL;
	storePos = NULL;
	storeLen = NULL;
	storeDat = NULL;
	storeLenSize = 0;
	storePosSize = 0;
	storeDatSize = 0;
	filterInfo = NULL;
	filterStrategy = NULL;
	fieldLen = NULL;
	size = 0;
	if (NULL == _handle || NULL == _filterInfo || NULL == _fieldlen || _size <= 0) {
		isInit = false;
		filterStrategy = NULL;
	} else {
		isInit = true;
		handle = _handle;
		filterInfo = _filterInfo;
		fieldLen = _fieldlen;
		size = _size;
		filterStrategy = new FilterStrategy(filterInfo);
		storeLenSize = handle->StoreLenSize;
		storePosSize = handle->StorePosSize;
		if (storeLenSize <= 0 || storePosSize <= 0) {
			isInit = false;
		} else {
			storePos = handle->StorePos;
			storeLen = handle->StoreLen;
		}
	}
}

DiskStoreFilter::~DiskStoreFilter() {
	if (NULL != filterStrategy) delete filterStrategy;
}

/* filter periods field, for this case, periods field may contains many time regions */
bool DiskStoreFilter::filterData(int id) {
	if (!isInit) return false;
	//printf("DiskStoreFilter::filterData id = %d\n", id);
	int blockNum = id/STORE_BLOCK_LEN;
	storeDatSize = handle->StoreDatSize[blockNum];
	storeDat = handle->StoreDat[blockNum];
	if (storeDatSize == 0 || NULL == storeDat) return false;

	unsigned char *storeLenStart = storeLen + sizeof(int)*id;
	if (storeLenStart + 4 > storeLen + storeLenSize) return false;
	unsigned char *storePosStart = storePos + sizeof(long)*id;
	if (storePosStart + 8 > storePos + storePosSize) return false;
	int len = *((int *)storeLenStart);                                      /* length of store data */
	long pos = *((long *)storePosStart);                                    /* position of store data */

	unsigned char *storeDatStart = storeDat + pos;
	if (storeDatStart > storeDat + storeDatSize) return false;
	unsigned char *storeDatEnd = storeDatStart + len;
	if (storeDatEnd > storeDat + storeDatSize) return false;
	int searchStoreFieldNum = (int)(*storeDatStart);                         /* number of field in store data */
	storeDatStart++;
	int i = 0, j = 0;
	unsigned char fileldId;
	int fLen = 0, fieldDataLen = 0;

	for ( ; j < filterInfo->filedNum && i < searchStoreFieldNum && storeDatStart <= storeDatEnd; i++ ) {
		fileldId =  *(storeDatStart++);
		if (fileldId >= size) return false;
		fieldDataLen = 0;
		fLen = 0;
		fLen = fieldLen[fileldId];
		if (fLen == 0) {
			int shift = 7, b = (*(storeDatStart++)) & 0xff;
			fieldDataLen = b & 0x7f;
			for ( ; (b & 0x80) != 0; shift += 7) {
				b = (*(storeDatStart++)) & 0xff;
				fieldDataLen |= (b & 0x7F) << shift;
			}
		} else {
			fieldDataLen = fLen;
		}

	//	printf("DiskStoreFilter::filterData fid = %d, fieldDataLen=%d filterInfo->filedNum=%d\n",fileldId, fieldDataLen, filterInfo->filedNum);

		if (fileldId == filterInfo->filedIds[j]) {
			if (fieldDataLen == 0) return true;
			if (fieldDataLen > MAX_STORE_LEN ) return false;
			bool ret = filterStrategy->filterStoreMethod((char *)storeDatStart, fieldDataLen, j);
			if (!ret) return false;
			storeDatStart += fieldDataLen;
			j ++;
		} else if (fileldId < filterInfo->filedIds[j]) {
			storeDatStart += fieldDataLen;
		} else return filterStrategy->returnStrategy(filterInfo->filedIds[j]);

	}
	
	if (j == filterInfo->filedNum) return true;
	else return false;
}

DiskFilter::DiskFilter(Handler *_handle, FilterInfo *_filterInfo) {                  /* read filter-query into DiskBrifeFilter and DiskStoreFilter */
	handle = NULL;
	delFile = NULL;
	delSize = 0;
	store = NULL;
	filterInfo = NULL;
	storeFilterInfo =NULL;
	indexField = NULL;
	storeSize = 0;
	fieldLen = NULL;
	docnum = 0;

	if (NULL == _handle || NULL == _filterInfo) {
		isInit = false;
		store = NULL;
		storeFilterInfo = NULL;
		indexField = NULL;
		fieldLen = NULL;
	} else {
		isInit = true;
		handle = _handle;
		filterInfo = _filterInfo;
		delFile = handle->DelDat;
		delSize = handle->DelDatSize;
		docnum = handle->docnum;
		if (NULL == handle->schema) {
			isInit = false;
		} else {
			indexField = (handle->schema)->toArray();
			int size = (handle->schema)->size();
			if (size > 0) {
				fieldLen = (int *)malloc(sizeof(int) * size);
				bzero(fieldLen, sizeof(int) * size);
			} else fieldLen = NULL;

			if (NULL == indexField) {
				isInit = false;
			} else {
				for (int i = 0; i < size; i++) {
					fieldLen[i] = indexField[i]->getLength();
				}
				bool isNeedStore = false;
				vector<int> storeFid;
				for (int i = 0; i < filterInfo->filedNum; i++) {                          /* brife filter */
					int fid = filterInfo->filedIds[i];
					if (fid >= size) isInit = false;
					else {
						if (!(indexField[fid]->isFilter())) {                             /* filter query not filter, add to store-filter */
							isNeedStore = true;
							storeFid.push_back(i);
							continue;
						}
						unsigned char * brifeobj = handle->Brifes[fid];
						int flen = fieldLen[fid];
						DiskBrifeFilter * brifeFilter = new DiskBrifeFilter(brifeobj, flen, fid, filterInfo->contents[i]);
						brifes.push_back(brifeFilter);
					}
				}
				
				storeSize = storeFid.size();                                              /* store filter */
				if (isNeedStore == true && storeSize > 0) {
					storeFilterInfo = (FilterInfo *)malloc(sizeof(FilterInfo));
					bzero(storeFilterInfo, sizeof(FilterInfo));
					if (NULL != storeFilterInfo) {
						storeFilterInfo->filedIds = (char *)malloc(sizeof(char) * storeSize);
						bzero(storeFilterInfo->filedIds, sizeof(char) * storeSize);
						storeFilterInfo->contents = (char **)malloc(sizeof(char *) * storeSize);
						bzero(storeFilterInfo->contents, sizeof(char *) * storeSize);
						storeFilterInfo->contLen = (int*)malloc(sizeof(int) * storeSize);
						bzero(storeFilterInfo->contLen, sizeof(int) * storeSize);
						if (NULL != storeFilterInfo->contents && NULL != storeFilterInfo->contLen) {
							for (int i = 0; i < storeSize; i++) {                      /* alloc space for storeFilterInfo */
								int idx = storeFid[i];
								int fid = filterInfo->filedIds[idx];
								//int flen = fieldLen[fid];
								//if (flen == 0) flen = MAX_CONTENT_SIZE; 
								int flen = filterInfo->contLen[idx];
								if (flen == 0) flen = MAX_CONTENT_SIZE; 
								storeFilterInfo->contents[i] = (char*)malloc(sizeof(char) * flen);
								if (NULL == storeFilterInfo->contents[i]) isInit = false;
								bzero(storeFilterInfo->contents[i], sizeof(char) * flen);
							}
						} else isInit = false;
					} else isInit = false;

					for (int i = 0; i < storeSize; i++) {                               /* copy content into storeFilterInfo */
						int idx = storeFid[i];
						int fid = filterInfo->filedIds[idx];
						//int flen = fieldLen[fid];
						storeFilterInfo->filedIds[i] = fid;
						int len = filterInfo->contLen[idx];
						storeFilterInfo->contLen[i] = len;
						memcpy(storeFilterInfo->contents[i], filterInfo->contents[idx], len);
						//printf("storeFilterInfo = %lld, filterInfo=%lld\n", *((long long *)storeFilterInfo->contents[i]), *((long long *)filterInfo->contents[i]));
					}
					storeFilterInfo->filedNum = storeSize;

					store = new DiskStoreFilter(handle, storeFilterInfo, fieldLen, size);

				} else {
					store = NULL;
					storeFilterInfo = NULL;
				}

			}
		}
	}
}

DiskFilter::~DiskFilter() {
	for (int i = 0; i < brifes.size(); i ++) {
		if (NULL != brifes[i]) delete brifes[i];
	}

	if (NULL != store) delete store;

	if (NULL != storeFilterInfo) {
		if (NULL != storeFilterInfo->filedIds) {
			free(storeFilterInfo->filedIds);
			storeFilterInfo->filedIds = NULL;
		}
		if (NULL != storeFilterInfo->contents) {
			for (int i = 0; i < storeSize; i++) {
				free(storeFilterInfo->contents[i]);
				storeFilterInfo->contents[i] = NULL;
			}
			free(storeFilterInfo->contents);
			storeFilterInfo->contents = NULL;
		}
		if (NULL != storeFilterInfo->contLen) {
			free(storeFilterInfo->contLen);
			storeFilterInfo->contLen = NULL;
		}
		free(storeFilterInfo);
		storeFilterInfo = NULL;
	}

	if (NULL != fieldLen) free(fieldLen); 
}

bool DiskFilter::filterData(int id) {                                        /* check period field, for this version */
	bool ret;
	if (!isInit) return false;
	if (isDelete(id)) return false;
	if (id >= docnum) return false;
//	printf("DiskFilter::filterData id = %d\n", id);

	for (int i = 0; i < brifes.size(); i++) {
		ret = brifes[i]->filterData(id);                                     /* brife filter */
		if (!ret) return false;
	}
	
	if (NULL != store) {
		ret = store->filterData(id);                                         /* store filter */
		if (!ret) return false;
	}

	if (brifes.size() == 0 && NULL == store) return false;

	return true;
}

bool DiskFilter::isDelete(int id) {                                           /* check del.dat */
	if (NULL == delFile || delSize == 0) return false;
	int off = id/8;
	char a = *(char *)(delFile + off);
	if ( (a & (1<<(7-id%8)))== (1<<(7-id%8)) ) return true;
	else return false;
}

RealBrifeFilter::RealBrifeFilter(RAMInputStream * _brife, int _len, int _fid, char * _content) {
	brifeDataStream = _brife;
	content = _content;
	brifeData = NULL;
	len = _len;
	fid = _fid;

	if (NULL == brifeDataStream || NULL == content || len <= 0) {
		isInit = false;
		brifeData = NULL;
		brifeDataStream = NULL;
	} else {
		isInit = true;
		brifeData = (char*)malloc(sizeof(char)*(len+1));
		bzero(brifeData, sizeof(char)*(len+1));
		if (NULL == brifeData) isInit = false;
	}
}

RealBrifeFilter::~RealBrifeFilter() {
	if (NULL != brifeData) free(brifeData);
	if (NULL != brifeDataStream) delete brifeDataStream;
}

bool RealBrifeFilter::filterData(int id) {
	int ret;
	//printf("RealBrifeFilter::filterData id = %d\n", id);
	if (!isInit) return false;

	//printf("RealBrifeFilter::filterData id = %d\n", id);
	int pos = id * len;
	ret = brifeDataStream->seekNow(pos);
	if (ret == 0) return false;
	bzero(brifeData, sizeof(char)*(len+1));
	ret = brifeDataStream->readBytesNow(brifeData, 0, len);
	if (ret < len) {
		int new_len = len - ret;
		long new_pos = ret;
		if (ret < 0) return false;
		ret = brifeDataStream->seekNow(pos + ret);
		if (ret == 0) return false;
		ret = brifeDataStream->readBytesNow(brifeData, new_pos , new_len);
		if (ret < new_len)  return false;
	}

	if (fid == CONT_TIME_FIELD) {                                 
		if (len != 4) return false;                               
		int data = *((int *)content);
		int indexdata = *((int *)(brifeData));
		if (indexdata < 0 ) return false;
		if (indexdata >= data) return true;
		else return false;
	}

	if (fid == PERIODS_FIELD) {
		if (len%PERIODS_BYTE_NUM != 0) return false;
		long long data = *((long long *)content);
		long long data1 = *((long long*)brifeData);
		long long data2 = *((long long *)(brifeData + PERIODS_BYTE_NUM/2));
		//printf("RealBrifeFilter::filterData data = %d, data1=%d, data2= %d\n", data, data1, data2);
		if (data1 < 0 || data2 < 0) return false;
		if (data1 == 0 && data2 == 0) return true;
		if (data >= data1 && data <= data2) return true;
		else return false;
	}

	return true;
}

RealStoreFilter::RealStoreFilter(Realtime * _realtime, FilterInfo * _filterInfo, int * _fieldlen, int _size) {
	realtime = NULL;
	storeLenStream = NULL;
	storePosStream = NULL;
	storeDatStream = NULL;
	filterInfo = NULL;
	filterStrategy = NULL;
	bzero(datBytes, MAX_STORE_LEN);
	size = 0;
	fieldLen = NULL;

	time1 = 0;
	time2 = 0;

	if (NULL == _realtime || NULL == _filterInfo || NULL == _fieldlen || _size <= 0) {
		isInit = false;
		storeLenStream = NULL;
		storePosStream = NULL;
		storeDatStream = NULL;
		filterStrategy = NULL;
	} else {
		isInit = true;
		realtime = _realtime;
		filterInfo = _filterInfo;
		fieldLen = _fieldlen;
		size = _size;
		filterStrategy = new FilterStrategy(filterInfo);
		storeLenStream = realtime->storeLenStream();
		storePosStream = realtime->storePosStream();
		storeDatStream = realtime->storeDatStream();
		if (NULL == this->storeLenStream || NULL == this->storePosStream || NULL == this->storeDatStream ) {
			this->isInit = false;
		}
	}
}

RealStoreFilter::~RealStoreFilter() {
	if (NULL != storeLenStream) delete(storeLenStream);
	if (NULL != storePosStream) delete(storePosStream);
	if (NULL != storeDatStream) delete(storeDatStream);
	if (NULL != filterStrategy) delete(filterStrategy);

	//printf("time1 = %d, time2=%d\n", time1, time2);
}

bool RealStoreFilter::filterData(int id) {
	int ret;
	int len, pos;
	char lenBytes[4] = {0};
	char posBytes[8] = {0};

	if (!isInit) return false;
	//printf("RealStoreFilter::filterData id = %d\n", id);

	struct  timeval begTime;
	struct  timeval endTime;
	gettimeofday(&begTime, NULL);

	ret = storeLenStream->seekNow(id * sizeof(int));
	if (ret == 0) return false;
	ret = storePosStream->seekNow(id * sizeof(long));
	if (ret == 0) return false;  
	ret = storeLenStream->readBytesNow(lenBytes, 0, sizeof(lenBytes));
	if (ret != sizeof(lenBytes)) return false;
	ret = storePosStream->readBytesNow(posBytes, 0, sizeof(posBytes));
	if (ret != sizeof(posBytes)) return false;
	len = *((int *)lenBytes);
	pos = *((long *)posBytes);
	if (len > MAX_STORE_LEN) return false;

	ret = storeDatStream->seekNow(pos);
	if (ret == 0) return false;
	bzero(datBytes, MAX_STORE_LEN);
	if (len > 52) len = 52;
	ret = storeDatStream->readBytesNow((char *)datBytes, 0, len);
	if (ret < len) {
		int new_len = len - ret;
		long new_pos = ret;
		if (ret < 0) return false;
		ret = storeDatStream->seekNow(pos + ret);
		if (ret == 0) return false;
		ret = storeDatStream->readBytesNow((char *)datBytes, new_pos , new_len);
		if (ret < new_len) 	return false;
	}

	gettimeofday(&endTime, NULL);
	int  diff = 1000000 * (endTime.tv_sec-begTime.tv_sec) + endTime.tv_usec-begTime.tv_usec;
	time1 += diff;

	gettimeofday(&begTime, NULL);
	unsigned char *storeDatStart = datBytes;
	unsigned char *storeDatEnd = datBytes + len * 8;
	int searchStoreFieldNum = (int)(*storeDatStart);
	storeDatStart++;
	int i = 0, j = 0;
	unsigned char fileldId;
	int fLen = 0,fieldDataLen = 0;
	for ( ; j < filterInfo->filedNum && i < searchStoreFieldNum && storeDatStart <= storeDatEnd; i++ ) {
		fileldId =  *(storeDatStart++);
		fieldDataLen = 0;
		fLen = 0;
		fLen = fieldLen[fileldId];
		if (fLen == 0) {
			int shift = 7, b = (*(storeDatStart++)) & 0xff;
			fieldDataLen = b & 0x7f;
			for (; (b & 0x80) != 0; shift += 7) {
				b = (*(storeDatStart++)) & 0xff;
				fieldDataLen |= (b & 0x7F) << shift;
			}  
		} else fieldDataLen = fLen;

		//printf("RealStoreFilter::filterData fid = %d, flen = %d, filterInfo->filedIds[j] = %d\n", fileldId, fieldDataLen, filterInfo->filedIds[j]);

		if (fileldId == filterInfo->filedIds[j]) {
			if (fieldDataLen == 0) return true;
			if (fieldDataLen > MAX_STORE_LEN) return false;
			ret = filterStrategy->filterStoreMethod((char *)storeDatStart, fieldDataLen, j);
			if (!ret) {
				gettimeofday(&endTime, NULL);
				diff = 1000000 * (endTime.tv_sec-begTime.tv_sec) + endTime.tv_usec-begTime.tv_usec;
				time2 += diff;
				return false;
			}
			storeDatStart += fieldDataLen;
			j ++;
		} else if (fileldId < filterInfo->filedIds[j]) {
			storeDatStart += fieldDataLen;
		} else return filterStrategy->returnStrategy(filterInfo->filedIds[j]);
	}

	gettimeofday(&endTime, NULL);
	diff = 1000000 * (endTime.tv_sec-begTime.tv_sec) + endTime.tv_usec-begTime.tv_usec;
	time2 += diff;
	if (j == filterInfo->filedNum) return true;
	else return false;
}


RealFilter::RealFilter(Realtime * _realtime, FilterInfo *_filterInfo) {                     /* read filter-query into RealBrifeFilter and RealStoreFilter */
	storeSize = 0;
	docnum = 0;
	realtime = NULL;
	store = NULL;
	filterInfo = NULL;
	storeFilterInfo = NULL;
	fieldLen = NULL;

	if (NULL == _realtime || NULL == _filterInfo) {
		isInit = false;
		store = NULL;
		storeFilterInfo = NULL;
		fieldLen = NULL;
	} else {
		isInit = true;
		realtime = _realtime;
		filterInfo = _filterInfo;
		if (NULL == realtime->schema) {
			isInit = false;
		} else {
			indexField =  realtime->schema->toArray();
			int size =  realtime->schema->size();
			if (size > 0) {
				fieldLen = (int *)malloc(sizeof(int) * size);
				bzero(fieldLen, sizeof(int) * size);
			} else fieldLen = NULL;

			if (NULL == indexField) isInit = false;
			else {
				for (int i = 0; i < size; i++) {
					fieldLen[i] = indexField[i]->getLength();
				}
				bool isNeedStore = false;
				vector<int> storeFid;
				for (int i = 0; i < filterInfo->filedNum; i++) {                            /* brifeFilter */
					int fid = filterInfo->filedIds[i];
					if (fid >= size) isInit = false;
					else {
						if (!(indexField[fid]->isFilter())) {                               /* filter query not filter, add to store-filter */
							isNeedStore = true;
							storeFid.push_back(i);
							continue;
						}

						//printf("filter brife = true\n");
						RAMInputStream * brifeStream = realtime->brifeSteams(fid);
						int flen = fieldLen[fid];
						RealBrifeFilter * brifeFilter = new RealBrifeFilter(brifeStream, flen, fid, filterInfo->contents[i]);
						brifes.push_back(brifeFilter);
					}
				}

				storeSize = storeFid.size();
				if (isNeedStore == true && storeSize > 0) {
					storeFilterInfo = (FilterInfo *)malloc(sizeof(FilterInfo));
					bzero(storeFilterInfo, sizeof(FilterInfo));
					if (NULL != storeFilterInfo) {
						storeFilterInfo->filedIds = (char *)malloc(sizeof(char) * storeSize);
						bzero(storeFilterInfo->filedIds, sizeof(char) * storeSize);
						storeFilterInfo->contents = (char **)malloc(sizeof(char *) * storeSize);
						bzero(storeFilterInfo->contents, sizeof(char *) * storeSize);
						storeFilterInfo->contLen = (int*)malloc(sizeof(int) * storeSize);
						bzero(storeFilterInfo->contLen, sizeof(int) * storeSize);
						if (NULL != storeFilterInfo->contents && NULL != storeFilterInfo->contLen) {
							for (int i = 0; i < storeSize; i++) {                           /* alloc space for storeFilterInfo */
								int idx = storeFid[i];
								int fid = filterInfo->filedIds[idx];
								//int flen = fieldLen[fid];
								int flen = filterInfo->contLen[idx];
								if (flen == 0) flen = MAX_CONTENT_SIZE;
								storeFilterInfo->contents[i] = (char*)malloc(sizeof(char) * flen);
								if (NULL == storeFilterInfo->contents[i]) isInit = false;
								bzero(storeFilterInfo->contents[i], sizeof(char) * flen);
							}
						} else isInit = false;
					} else isInit = false;

					for (int i = 0; i < storeSize; i++) {                                    /* copy content */
						int idx = storeFid[i];
						int fid = filterInfo->filedIds[idx];
						//int flen = fieldLen[fid];
						storeFilterInfo->filedIds[i] = fid;
						int len = filterInfo->contLen[idx];
						storeFilterInfo->contLen[i] = len;
						memcpy(storeFilterInfo->contents[i], filterInfo->contents[idx], len);
					}
					storeFilterInfo->filedNum = storeSize;

					store = new RealStoreFilter(realtime, storeFilterInfo, fieldLen, size);

				} else {
					store = NULL;
					storeFilterInfo = NULL;
				}
			}
		}
	}
}

RealFilter::~RealFilter() {
	for (int i = 0; i < brifes.size(); i ++) {
		if (NULL != brifes[i]) delete brifes[i];
	}

	if (NULL != store) delete store;

	if (NULL != storeFilterInfo) {
		if (NULL != storeFilterInfo->filedIds) {
			free(storeFilterInfo->filedIds);
			storeFilterInfo->filedIds = NULL;
		}
		if (NULL != storeFilterInfo->contents) {
			for (int i = 0; i < storeSize; i++) {
				free(storeFilterInfo->contents[i]);
				storeFilterInfo->contents[i] = NULL;
			}
			free(storeFilterInfo->contents);
			storeFilterInfo->contents = NULL;
		}
		if (NULL != storeFilterInfo->contLen) {
			free(storeFilterInfo->contLen);
			storeFilterInfo->contLen = NULL;
		}
		free(storeFilterInfo);
		storeFilterInfo = NULL;
	}

	if (NULL != fieldLen) free(fieldLen);
}

bool RealFilter::filterData(int id) {
	bool ret;
	if (!isInit) return false;
	if (realtime->isDeleted(id)) return false;
	docnum = realtime->getDocnum();
	if (id >= docnum) return false;

	//printf("RealFilter::filterData id = %d\n", id);

	for (int i = 0; i < brifes.size(); i++) {
		ret = brifes[i]->filterData(id);
		//printf("RealFilter::filterData ret = %d\n", ret);
		if (!ret) return false;
	}

	if (NULL != store) {
		//printf("RealFilter::filterData store\n");
		ret = store->filterData(id);
		if (!ret) return false;
	}

	if (brifes.size() == 0 && NULL == store) return false;

	//printf("RealFilter::filterData true\n");
	return true;
}

FilterStrategy::FilterStrategy(FilterInfo * _filterInfo) {
	filterInfo = _filterInfo;
}

FilterStrategy::~FilterStrategy() {
}

bool FilterStrategy::returnStrategy(int fid) {
	if (fid == PERIODS_FIELD) return true;
	else return false;
}

/* filter periods field, for this case, periods field may contains many time regions */
bool FilterStrategy::filterStoreMethod(char * storeData, int storeLen, int idx) {
	if (NULL == filterInfo || NULL == storeData || storeLen <= 0) return false;
	if (idx >= filterInfo->filedNum) return false;
	if (NULL == filterInfo->contents[idx]) return false;

	if (filterInfo->filedIds[idx] == PERIODS_FIELD) {
		if (storeLen%PERIODS_BYTE_NUM != 0) return false;
		long long data = *((long long *)filterInfo->contents[idx]);
		int num = storeLen/PERIODS_BYTE_NUM;
		for (int i = 0; i < num; i++) {
			long long data1 = *((long long*)(storeData + i * PERIODS_BYTE_NUM));
			long long data2 = *((long long*)(storeData + i * PERIODS_BYTE_NUM + PERIODS_BYTE_NUM/2));
			//printf("FilterStrategy::filterMethod  data = %lld, data1 = %lld, data2 = %lld\n", data, data1,data2);
			if (data1 < 0 || data2 < 0) return false;
			if (data >= data1 && data <= data2) return true;                             /* filter query is in one time region */
		}
		return false;
	}
	return false;
}

bool FilterStrategy::filterBrifeMethod(char * brifeData, int len, char *content, int fid) {
	if (fid == CONT_TIME_FIELD) {
		if (len != 4) return false;
		int data = *((int *)content);
		int indexdata = *((int *)(brifeData));
		if (indexdata < 0 ) return false;
		if (indexdata >= data) return true;
		else return false;
	}
	return false;
}
