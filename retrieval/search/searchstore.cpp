#include "searchstore.h"

SearchStore::SearchStore(IndexHandler * handle) {
	StoreLen = NULL;
	StorePos = NULL;
	StoreDat = NULL;
	StoreLenSize = 0;
	StorePosSize = 0;
	StoreDatSize = 0;
	indexHandle = NULL;
	indexField = NULL;
	fullTermDocsMaxSize = 0;
	incTermDocsMaxSize = 0;
	needSeekFull = 0;
	needSeekIncr = 0;
	needSeekRt = 0;
	initialized = 0;
	if (NULL == handle)	{
		initialized = 0;
		indexHandle = NULL;
	} else {
		fullTermDocsMaxSize = 0;
		incTermDocsMaxSize = 0;
		needSeekFull = 0;
		needSeekIncr = 0;
		needSeekRt = 0;
		StoreLen  = NULL;
		StoreLenSize = 0;
		StorePos = NULL;
		StorePosSize = 0;
		StoreDatSize = 0;

		if (NULL != handle->fullHandler()) {
			fullTermDocsMaxSize = (handle->fullHandler())->docnum;
			needSeekFull = 1;
		}
		if (NULL != handle->incrHandler()) {
			incTermDocsMaxSize = (handle->incrHandler())->docnum;
			needSeekIncr = 1;
		}
		if (NULL != handle->realtimeHandler()) {
			needSeekRt = 1;
		}

		initialized = 1;
		indexHandle = handle;
	}
}

SearchStore::~SearchStore() {
}

vector< vector<unsigned char> > SearchStore::seek(int DocID, vector<unsigned char> Fields) {
	vector< vector<unsigned char> > Result;
	Handler * handle = NULL;
	Realtime * realTime = NULL;
	int docId;
	Result.clear();
	if(initialized == false) return Result; 
	if (DocID < 0 || Fields.size() == 0) return Result;

	if (fullTermDocsMaxSize != 0 && DocID < fullTermDocsMaxSize && needSeekFull == 1) {
		handle = indexHandle->fullHandler();
		docId = DocID;
	} else if ((DocID >= fullTermDocsMaxSize) && incTermDocsMaxSize != 0 &&
				DocID < fullTermDocsMaxSize + incTermDocsMaxSize && needSeekIncr == 1) {
		handle = indexHandle->incrHandler();
		docId = DocID - fullTermDocsMaxSize;
	} else if ((DocID >= fullTermDocsMaxSize + incTermDocsMaxSize) && needSeekRt == 1) {
		realTime = indexHandle->realtimeHandler();
		docId = DocID - fullTermDocsMaxSize - incTermDocsMaxSize;
		if (realTime->isDeleted(docId))	{
			Result.clear();
			return Result;
		}

		Result = seekRealtime(realTime, Fields, docId);
		return Result;
	}

	if (NULL == handle)	{
		Result.clear();
		return Result;
	}

	Result = seekDiskdata(handle, Fields, docId);
	return Result;
}

/* Result[i] is the store-data of Fields[i] */
vector< vector<unsigned char> > SearchStore::seekDiskdata(Handler * handle, vector<unsigned char> Fields, int docId) {
	vector< vector<unsigned char> > Result;
	if (NULL == handle || Fields.size() == 0) return Result;

	StorePosSize = handle->StorePosSize;
	StoreLenSize = handle->StoreLenSize;
	StoreLen = handle->StoreLen;
	StorePos = handle->StorePos;
	if (StorePosSize == 0 || StoreLenSize == 0 || NULL == StoreLen || NULL == StorePos) {
		Result.clear();
		return Result;
	}
	if (NULL == handle->schema) {
		return Result;
	} else {
		indexField = (handle->schema)->toArray();
		if (NULL == indexField) {
			return Result;
		}
	}
	
	int blockNum = docId/STORE_BLOCK_LEN;
	StoreDatSize = handle->StoreDatSize[blockNum];                              /* size of store.dat.blockNum */
	StoreDat = handle->StoreDat[blockNum];                                      /* store.dat.blockNum */
	if (StoreDatSize == 0 || NULL == StoreDat) {
		Result.clear();
		return Result;
	}

	unsigned char *StoreLenStart = StoreLen + sizeof(int)*docId; 
	if (StoreLenStart + 4 > StoreLen + StoreLenSize) {
		Result.clear();
		return Result;
	}
	unsigned char *StorePosStart = StorePos + sizeof(long)*docId;
	if (StorePosStart + 8 > StorePos + StorePosSize) {
		Result.clear();
		return Result;
	}
	int len = *((int *)StoreLenStart);                                           /* length of store data */
	long pos = *((long *)StorePosStart);                                         /* shift of store data in store.dat.blockNum */

	Result = buildResult(StoreDat, pos, len, Fields);
	return Result;
}

/* read store.dat, store store-data of all fields in Fields into Result  */
vector< vector<unsigned char> > SearchStore::buildResult(unsigned char * databuf, int offset, int len, vector<unsigned char> Fields) {
	vector< vector<unsigned char> > Result;
	if (NULL == databuf || len <= 0 || Fields.size() == 0) return Result;
	unsigned char *StoreDatStart = databuf + offset;
	unsigned char *StoreDatEnd = StoreDatStart + len;
	int SearchStoreFieldNum = (int)(*StoreDatStart);                                             /* count of field */
	StoreDatStart++;
	int i = 0;
	int j = 0;
	int fieldLenLen = 0;
	int fieldDataLen = 0;
	int fieldLen = 0;
	unsigned char FieldId;
	vector<unsigned char> TmpData;
	for (; j < Fields.size() && i < SearchStoreFieldNum && StoreDatStart <= StoreDatEnd; i++ ) {
		FieldId = *(StoreDatStart++);
		fieldLenLen = 0;
		fieldDataLen = 0;
		fieldLen = 0;

		if (NULL == indexField[FieldId]) {
			 Result.clear();
			 return Result;
		}
		fieldLen = indexField[FieldId]->getLength();
		if (fieldLen == 0) {
			int shift = 7, b = (*(StoreDatStart++)) & 0xff;
			fieldDataLen = b & 0x7f;
			for (; (b & 0x80) != 0; shift += 7) {
				b = (*(StoreDatStart++)) & 0xff;
				fieldDataLen |= (b & 0x7F) << shift;
			}
		} else fieldDataLen = fieldLen;

		if (FieldId == Fields[j]) {
			TmpData.clear();
			while (fieldDataLen > 0) {                                              /* push store data into TmpData */
				TmpData.push_back(*StoreDatStart);
				StoreDatStart++;
				fieldDataLen--;
			}
			Result.push_back(TmpData);                                              /* store store-data of all fields in Fields into Result */
			j++;
		} else if (FieldId < Fields[j]) {
			StoreDatStart += fieldDataLen;
		} else {                                                                    /* field not in store-data, keep it empty in Result */
			TmpData.clear();
			Result.push_back(TmpData);
			j++;
			while (j < Fields.size() && FieldId > Fields[j]) {
				Result.push_back(TmpData);
				j ++;
			}
			if (FieldId < Fields[j]) {
				StoreDatStart += fieldDataLen;
				continue;
			}
			if (FieldId == Fields[j]) {
				while (fieldDataLen > 0) {
					TmpData.push_back(*StoreDatStart);
					StoreDatStart++;
					fieldDataLen--;
				}
				Result.push_back(TmpData);
				j++;   
			}
		}
	}

	return Result;
}

vector< vector<unsigned char> > SearchStore::seekRealtime(Realtime *realTime, vector<unsigned char> Fields, int docId) {
	vector< vector<unsigned char> > result;
	int ret;
	int errFlag = 0;
	int len = 0;
	long pos = 0;
	char lenBytes[4] = {0};
	char posBytes[8] = {0};
	unsigned char datBytes[MAX_STORE_LEN] = {0};
	if (NULL == realTime || Fields.size() == 0 || docId < 0) return result;

	RAMInputStream * lenStream = NULL;
	RAMInputStream * posStream = NULL;
	RAMInputStream * datStream = NULL;
	lenStream = realTime->storeLenStream();
	posStream = realTime->storePosStream();
	datStream = realTime->storeDatStream();
	if (NULL == lenStream || NULL == posStream || NULL == datStream) goto EXIT_PRO;   
	if (NULL == realTime->schema) goto EXIT_PRO;
	else {
		indexField =  realTime->schema->toArray();
		if (NULL == indexField) goto EXIT_PRO ;
	}

	ret = lenStream->seekNow(docId * sizeof(int));
	if (ret == 0) goto EXIT_PRO;
	ret = posStream->seekNow(docId * sizeof(long));
	if (ret == 0) goto EXIT_PRO;
	ret = lenStream->readBytesNow(lenBytes, 0, sizeof(lenBytes));
	if (ret != sizeof(lenBytes)) goto EXIT_PRO ;
	ret = posStream->readBytesNow(posBytes, 0, sizeof(posBytes));
	if (ret != sizeof(posBytes)) goto EXIT_PRO ;
	len = *((int *)lenBytes);
	pos = *((long *)posBytes);

	ret = datStream->seekNow(pos);
	if (ret == 0) goto EXIT_PRO;
	ret = datStream->readBytesNow((char *)datBytes, 0, len);
	if (ret < len) {
		int new_len = len - ret;
		long new_pos = ret;
		if (ret < 0) goto EXIT_PRO;
		ret = datStream->seekNow(pos + ret);
		if (ret == 0) goto EXIT_PRO;
		ret = datStream->readBytesNow((char *)datBytes, new_pos , new_len);
		if (ret < new_len) goto EXIT_PRO;
	}

	result = buildResult(datBytes, 0, len, Fields);

EXIT_PRO :
	if (NULL != lenStream) free(lenStream);
	if (NULL != posStream) free(posStream);
	if (NULL != datStream) free(datStream);
	return result;
}


