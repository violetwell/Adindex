#include "brifereader.h"

BrifeReader::BrifeReader() {
}

BrifeReader::~BrifeReader() {
}

DiskBrifeReader::DiskBrifeReader(Handler *handle, vector<int> fields, int base) {           /* read brife data of field in fields from handle.Brifes */
	isInit = true;
	if (NULL == handle || fields.size() == 0) {
		isInit = false;
	} else {
		this->handle = handle;
		this->base = base;
		docnum = handle->docnum;
		if (NULL == handle->schema) {
			isInit = false;
		} else {
			indexField = (handle->schema)->toArray();
			size = (handle->schema)->size();
			if (NULL == indexField || size <= 0) {
				isInit = false;
			} else {
				for (int i = 0; i < size; i++) {
					brifes.push_back(NULL);
					fieldlen.push_back(0);
				}
				for (int i = 0; i < fields.size(); i++) {                                    /* for fieldId in fields */
					int fid = fields[i];
					if (fid >= size) isInit = false;
					else {
						brifes[fid] = handle->Brifes[fid];                                   /* read brife data */
						fieldlen[fid] = indexField[fid]->getLength();
					}
				}
			}
		}
	}
}

DiskBrifeReader::~DiskBrifeReader() {
}

char* DiskBrifeReader::getDiskBrife(int docid, int fid, int len) {                       /* return data of field: fid of doc: docid - base */
	if (!isInit) return NULL;
	if (docid < base || docid >= (base + docnum)) return NULL;
	if (fid >= size) return NULL;
	if (len == 0 || fieldlen[fid] != len) return NULL;

	int id = docid - base;
	unsigned char * brife = brifes[fid];
	int pos = id * len;
	unsigned char * brifestart = brife + pos;

	return (char *)brifestart;
}

RealBrifeReader::RealBrifeReader(Realtime * realtime, vector<int> fields, int base) {
	isInit = true;
	if (NULL == realtime || fields.size() == 0) {
		isInit = false;
	} else {
		this->realtime = realtime;
		this->base = base;
		//docnum = realtime->getDocnum();
		if (NULL == realtime->schema) {
			this->isInit = false;
		} else {
			indexField =  realtime->schema->toArray();
			size = realtime->schema->size();
			if (NULL == indexField || size <= 0) isInit = false;
			else {
				for (int i = 0; i < size; i++) {
					brifes.push_back(NULL);
					fieldlen.push_back(0);
				}
				for (int i = 0; i < fields.size(); i++) {
					int fid = fields[i];
					if (fid >= size) isInit = false;
					else {
						RAMInputStream * brifeStream = realtime->brifeSteams(fid);
						if (NULL == brifeStream) isInit = false;
						else {
							brifes[fid] = brifeStream;
							fieldlen[fid] = indexField[fid]->getLength();
						}
					}
				}
			}
		}
	}
}

RealBrifeReader::~RealBrifeReader() {
	for (int i = 0; i < size; i++) {
		RAMInputStream * brifestream = brifes[i];
		if (NULL != brifestream) delete(brifestream);
	}
}

bool RealBrifeReader::getRealBrife(int docid, int fid, char* retbuf, int len) {       /* return data of field: fid of doc: docid - base */
	int ret;
	if (!isInit) return false;
	docnum = realtime->getDocnum();
	if (docid < base || docid >= (base + docnum)) return false;
	if (fid >= size) return false;
	if (NULL == retbuf) return false;
	if (len == 0 || len != fieldlen[fid]) return false;
	int id = docid - base;
	if (realtime->isDeleted(id)) return false;

	RAMInputStream * brifestream = brifes[fid];
	if (NULL == brifestream) return false;

	int pos = id * len;
	ret = brifestream->seekNow(pos);
	if (ret == 0) return false;
	ret = brifestream->readBytesNow(retbuf, 0, len);
	if (ret < len) {
		int new_len = len - ret;
		long new_pos = ret;
		if (ret < 0) return false;
		ret = brifestream->seekNow(pos + ret);
		if (ret == 0) return false;
		ret = brifestream->readBytesNow(retbuf, new_pos , new_len);
		if (ret < new_len) 	return false;
	}
	return true;
}
