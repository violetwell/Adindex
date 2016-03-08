#ifndef  __REALTIME_H_
#define  __REALTIME_H_

#include "def.h"
#include "adindex.h"
#include <map>
#include <time.h>
#include <ctime>
using namespace std;

typedef struct _TermHash_t {
    int *ords;                                                         /* ords[i] store the termId of term which hash to i, ords[i]=-1 if not used */
	int hashSize;                                                      /* size of hash table */
	int hashHalfSize;
	int hashMask;
} TermHash;

class TermDict {

public:
	TermDict(ByteBlockPool *_termPool, ByteBlockPool *_dnftermPool, SimpleGc *_gc);
	~TermDict();
	int addTerm(TermInfo terminfo);
	int getTerm(TermInfo terminfo);
	int addDnfTerm(TermInfo terminfo);
	int getDnfTerm(TermInfo terminfo);
	int getTerm(Term terminfo);
	int getDnfTerm(Term terminfo);
	RawPostingList *getPostList(int termId);
	DnfRawPostingList *getDnfPostList(int dnftermId);
	int count;                                                         /* total count of terms in positions */
	int dnfcount;                                                      /* total count of dnf terms in dnfpositions */
	TermHash *termHash;                                                /* hash related data */
	TermHash *dnftermHash;
	
private:
	ByteBlockPool *termPool;                                           /* init with realtime.termPool, store TermInfo.field(field-id 1byte),data(6byte for tag) */
	RawPostingList **positions;                                        /* array of pointers, each points to array of size 10240 which store term in order of termId */
	int positionNum;                                                   /* count of pointers in positions */
	ByteBlockPool *dnftermPool;                                        /* init with realtime.dnftermPool, store TermInfo.field(conjunction-size 1byte),data(key 2byte,data 6byte) */
	DnfRawPostingList **dnfpositions;                                  /* array of pointers, each points to array of size 10240 which store dnfterm in order of termId */
	int dnfpositionNum;                                                /* count of pointers in dnfpositions */
	SimpleGc *gc;
	
	void nextBuffer();
	void nextDnfBuffer();
	bool postingEquals(int e, char field, char* tokenText, int tokenTextLen);
	bool dnfpostingEquals(int e, char field, char* tokenText, int tokenTextLen);
	void rehashPostings(int newSize);
	void rehashDnfPostings(int newSize);
	
};

class RamFile {
	
public:
	RamFile(SimpleGc *_gc);
	~RamFile();
	long getLength();
	void setLength(long _length);
	char* addBuffer(int size);
	char* getBuffer(int index);
	int numBuffers();
	char* newBuffer(int size);
	long getSizeInBytes();
	
private:
	char **dataBuffer;
	int bufferLen;                                   /* count of used buffer pointers in dataBuffer, count of alloced buffer */
	int bufferSize;                                  /* size of dataBuffer */
	long length;                                     /* size of written buffer in byte, used to write file */
	long sizeInBytes;                                /* size of alloced buffer in byte */
	SimpleGc *gc;
	
};

class RAMOutputStream {
	
public:
	RAMOutputStream(SimpleGc *gc);
	~RAMOutputStream();
	int writeFile(FILE *file);
	void reset();
	void close();
	void seek(long pos);
	long length();
	void writeByte(char b);
	void writeBytes(char *b, int offset, int len);
	void flush();
	long getFilePointer();
	long sizeInBytes();
	RamFile *ramfile;
	
private:
	char *currentBuffer;
	int currentBufferIndex;                           /* currentBuffer = RamFile.dataBuffer[currentBufferIndex] */
	int bufferPosition;                               /* position to write in currentBuffer */
	long bufferStart;                                 /* total shift in RamFile.dataBuffer */
	int bufferLength;                                 /* length of currentBuffer */
	void switchCurrentBuffer();
	void setFileLength();
	
};

class RAMInputStream {
	
public:
	RAMInputStream(RamFile *ramfile);
	~RAMInputStream();
	void close();
	long length();
	char readByte();
	int readBytes(char *b, int offset, int len);
	long getFilePointer();
	void seek(long pos);
	int seekNow(long pos);
	char readByteNow();
	int readBytesNow(char *b, int offset, int len);
	
private:
	RamFile *ramfile;
	long filelength;                                    /* RamFile.length */
	char *currentBuffer;                                /* currentBuffer = RamFile.dataBuffer[currentBufferIndex] */
	int currentBufferIndex;
	int bufferPosition;                                 /* position to read in currentBuffer */
	long bufferStart;                                   /* total shift in RamFile.dataBuffer */
	int bufferLength;                                   /* length of data bytes in currentBuffer, might be less than RAMFILE_BUFFER_SIZE */
	int switchCurrentBuffer(bool enforceEOF);
	
};

class Realtime {
	
public:
	Realtime(void *_conf);
	~Realtime();
	int ready();
	int close();
	int addDoc(char *data, int dataLen);
	int deleteDoc(int uid);
	RAMInputStream* brifeSteams(int i);
	RAMInputStream* storeLenStream();
	RAMInputStream* storePosStream();
	RAMInputStream* storeDatStream();
	bool isDeleted(int docid);
	void markDeleted(int docid);
	
	int getDocnum();
	int getConjnum();
	int getMaxConjSize();
	
	ByteBlockPool *termPostPool;                               /* store docId(VInt),payload(2byte) */
	IntBlockPool *dnfconjPostIndex;                            /* store start, end of the post list in dnfconjPostPool in conjId order */
	ByteBlockPool *dnfconjPostPool;                            /* store docId(VInt) */
	ByteBlockPool *dnftermPostPool;                            /* store conjId(VInt),payload(belong 1byte) */
	TermDict *termDict;                                        /* term related data, including hash, postlist, termpool */
	ByteBlockPool *termPool;                                   /* store TermInfo.field(field-id 1byte),data(6byte for tag) */
	ByteBlockPool *dnftermPool;                                /* store TermInfo.field(conjunction-size 1byte),data(key 2byte,data 6byte) */
	IndexSchema *schema;
	
private:
	bool readyflag;
	int docnum;                                                /* number of doc in realtime */
	int conjnum;                                               /* number of conj in realtime */
	int maxconjsize;
	DocParser *parser;
	IndexField *keyField;
	
	char *indexPath;
	map<int, int> uidmap;                                      /* map<uid, docId> for docs stored in realtime-index */
	char **bitmap;                                             /* delRef of realtime-index, each docId takes a bit */
	int bitmapLen;                                             /* count of pointers(array of 1024 byte) in bitmap */
	DelRef *fullDelRef;
	DelRef *incrDelRef;
	
	int indexFieldNumber;
	IndexField **indexField;                                   /* store index-field */
	Analyzer **analyzers;                                      /* analyzers[i] analyse indexField[i] */
	
	int brifeFieldNumber;
	IndexField **brifeField;                                   /* store brife-field */
	RAMOutputStream **brifeOutput;                             /* brifeOutput[i] belongs to ith field in schema.xml */

	int storeFieldNumber;
	IndexField **storeField;                                   /* store store-field */
	RAMOutputStream *storeLenOutput;
	RAMOutputStream *storePosOutput;
	RAMOutputStream *storeDatOutput;
	char *storeBuffer;
	int storeLen;
	
	TermInfo **infos;                                           /* infos[i] points to array of TermInfo of non-dnf terms */
	int *infoLens;                                              /* count of term in infos[i] */
	int infoNum;                                                /* count of added field in infos */
	
	TermInfo* dnfterms;                                         /* terms extracted from dnf field */
	int *conjIds;                                               /* conjIds[i] is the conjId of dnfterms[i] */
	int newIdnum;
	int oldIdnum;
	int *newIds;
	int *oldIds;
	int dnftermsNum;
	
	Allocator *allocator;
	RawPostingList *p;
	DnfRawPostingList *dnfp;
	
	int lastGc;
	SimpleGc *gc;
	RtByteSliceReader *reader;
	
	int doBrife();
	int doStore();
	int doInverted();
	int doDelete(int uid);
	void writeInt(RAMOutputStream *output, int value);
	void writeVint(RAMOutputStream *output, int value);
	void writeLong(RAMOutputStream *output, long value);
	void addTerms();
	void termVInt(int i);
	void termByte(char b);
	void addDnfTerms();
	void conjVInt(int off, int i);
	void conjByte(int off, char b);
	void dnftermVInt(int i);
	void dnftermByte(char b);
	int textanalyze(char *data, int length, int fieldId);
	int tagsanalyze(char *data, int length, int fieldId);
	int dnfanalyze(char *data, int length, int fieldId);
	int saveBrife();
	int saveStore();
	int saveIndex();
	int saveDnfIndex();
	int saveConjuction();
	int saveDelete();
	int* compact(TermHash *current, int count);
	void sortposting(int *ords, int lo, int hi);
	int RawPostingListCmp(int a, int b);
	void sortdnfposting(int *ords, int lo, int hi);
	int DnfRawPostingListCmp(int a, int b);
	
};

#endif
