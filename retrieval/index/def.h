#ifndef  __DEF_H_
#define  __DEF_H_

#define RAMFILE_BUFFER_SIZE 10240
#define INDEX_DEL "del.dat"
#define INDEX_TERM_SKIP "term.idx"
#define INDEX_TERM_DAT "term.dat"
#define INDEX_INVERT_DAT "index.dat"
#define CONJUCTION_INDEX "conj.idx"
#define CONJUCTION_DAT "conj.dat"
#define DNF_INDEX_TERM_SKIP "dnfterm.idx"
#define DNF_INDEX_TERM_DAT "dnfterm.dat"
#define DNF_INDEX_INVERT_DAT "dnfindex.dat"
#define STORE_FILE_NAME "store.dat"
#define STORE_POS_FILE_NAME "store.pos"
#define STORE_LEN_FILE_NAME "store.len"
#define MAX_CONJSIZE "max.conjsize"
#define MAX_INDEX_FIELD 100
#define MAX_STORE_LENGTH 102400
#define INT_BLOCK_SHIFT 13
#define INT_BLOCK_SIZE 8192
#define INT_BLOCK_MASK 8191
#define BYTE_BLOCK_SHIFT 15
#define BYTE_BLOCK_SIZE 32768
#define BYTE_BLOCK_MASK 32767
#define BYTE_BLOCK_NOT_MASK -32768
#define GROUP_LEFT '('
#define GROUP_RIGHT ')'
#define BELONG '<'
#define NOT_BELONG '>'
#define ATTRIBUTE_LEFT '{'
#define ATTRIBUTE_RIGHT '}'
#define DNF_AND '+'
#define DNF_OR '-'
#define ATTRIBUTE_SPLIT ';'
#define MAX_TAGS_NUMBER 1000
#define MAX_DNF_NUMBER 3000
#define MAX_CONJ_NUMBER 100
#define KEY_SIZE "key-size"
#define RAWFILENAME "rawfile"
#define READ_BUFFER_LENGTH 8192
#define MAX_DOC_LENGTH 20480
#define DOC_BEGIN 126
#define DOC_END 127
#define INDEX_FIELD_INT 0
#define INDEX_FIELD_STR 1
#define INDEX_FIELD_DNF 2
#define INDEX_FIELD_TAG 3
#define INDEX_FIELD_BIN 4
#define INDEX_FIELD_INT_STR "int"
#define INDEX_FIELD_STR_STR "str"
#define INDEX_FIELD_DNF_STR "dnf"
#define INDEX_FIELD_TAG_STR "tag"
#define INDEX_FIELD_BIN_STR "bin"
#define INDEX_SCHEMA_NAME "name"
#define INDEX_SCHEMA_TYPE "type"
#define INDEX_SCHEMA_LENGTH "length"
#define INDEX_SCHEMA_INDEXED "indexed"
#define INDEX_SCHEMA_STORED "stored"
#define INDEX_SCHEMA_FILTER "filter"
#define INDEX_SCHEMA_KEY "key"
#define MAX_DIR_LEN 1024
#define REALTIME_BITMAP_LEN 1024
#define MAX_STORE_UNIT 1024
#define MAX_STORE_DOCNUM 10000000
#define LOAD_TERM_NUM 128
#define MAX_TERM_LENGTH 128

#include <stdlib.h>
#include <string.h>
#include <string>
#include <time.h>
#include <ctime>

typedef struct _Store_Pos_t {
    long pos;     
} Store_Pos;

typedef struct _Store_Length_t {
    int length;
} Store_Length;

typedef struct _Term_Info {
	char field;                              /* index of the term in schema.xml for non-dnf term; size of conjunction for dnf term */
	char *data;                              /* data of the term */
	int dataLen : 16;                        /* data length of the term */
	char *payload;                           /* ub of the term */
	int payloadLen : 16;
} TermInfo;

typedef struct _Term {
	unsigned char field;
	unsigned char *data;
	int dataLen : 16;
} Term;

typedef struct _Term_Dat {
	char *data;
	int dataLen : 16;
	int docNum;
	long offset;
	float ub;
} TermDat;

typedef struct _RAW_POSTING_LIST {
	int textStart;
	short termLength;
	int invertedStart;
	int invertedUpto;
	float ub;                                /* UB of the term */
} RawPostingList;

typedef struct _DNF_RAW_POSTING_LIST {
	int textStart;                           /* point to the start of term data in dnftermPool */
	int invertedStart;                       /* point to the start of conjId, payload in dnftermPostPool */
	int invertedUpto;                        /* point to the end of conjId, payload in dnftermPostPool */
} DnfRawPostingList;

class DocIDMapper {
	
public:
	DocIDMapper(long *_uidArray, int uidArrayLen);
	~DocIDMapper();
	int getDocID(long uid);
	static int longCompare(const void *a, const void *b);
	
private:
	int *docArray;
	long *uidArray;
	int *start;
	long *filter;
	int mask;
	int MIXER;
	int findIndex(long *arr, long uid, int begin, int end);
	
};

typedef struct _Del_Ref {
	char *bitdata;
	int length;
	char *filePath;
	DocIDMapper *mapper;
} DelRef;

class SimpleGc {
	
public:
	SimpleGc();
	~SimpleGc();
	void addPointer(void *pointer);
	void doCollect();
	
private:
	void **toFree;
	int *inTime;
	int size;
	int used;
	
};

typedef struct _Merge_Info {
	int cur;
	int id;
} MergeInfo;

typedef struct _Merge_Conj_Info {
	long offset;
	int length;
} MergeConjInfo;

#endif
