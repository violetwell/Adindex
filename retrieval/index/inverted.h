#ifndef  __INVERTED_H_
#define  __INVERTED_H_

#include "def.h"
#include "adindex.h"

class Inverted {
	
public:
	Inverted(void *_conf);
	~Inverted();
	int close();
	int index(DocParser *parser);
	
private:
	IndexSchema *schema;
	int docid, realdocid;
	int indexFieldNumber;
	IndexField **indexField;                     /* store the index IndexField in schema*/
	Analyzer **analyzers;                        /* analyzers[i] points to the analyzer for ith index_field */
	char *path;
	char *fullDict;
	int fileNumber;
	int maxdoc;
	
	TermInfo **infos;                            /* store non-dnf terms from analyzer*/
	int *infoLens;                               /* infoLens[i] is the count of termInfo in infos[i] */
	int infoNum;                                 /* length of infos and infoLens */
	
	TermInfo* dnfterms;                          /* store dnf terms from analyzer */
	int *conjIds;
	int newIdnum;
	int oldIdnum;
	int *newIds;
	int *oldIds;
	int dnftermsNum;                             /* num of terms in dnfterms */
	int maxconjsize;
	
	ByteBlockPool *termPool;                     /* store TermInfo.field(field-id 1byte),data(6byte for tag) */
	ByteBlockPool *termPostPool;                 /* store docId(VInt),payload(2byte) */
	Allocator *allocator;
	RawPostingList **postingsHash;
	RawPostingList *p;
	RawPostingList **freePostings;
	int numPostings;
	int postingsHashSize;
	int postingsHashHalfSize;
	int postingsHashMask;
	int freePostingsCount;
	ByteSliceReader *reader;
	
	IntBlockPool *dnfconjPostIndex;              /* store start, end of the post list in dnfconjPostPool in conjId order */
	ByteBlockPool *dnfconjPostPool;              /* store docId(VInt) */
	ByteBlockPool *dnftermPool;                  /* store TermInfo.field(conjunction-size 1byte),data(key 2byte,data 6byte) */
	ByteBlockPool *dnftermPostPool;              /* store conjId(VInt),payload(belong 1byte) */
	DnfRawPostingList **dnfpostingsHash;         /* hash list of DnfRawPostingList */
	DnfRawPostingList *dnfp;
	DnfRawPostingList **dnffreePostings;
	int dnfnumPostings;                          /* length of dnfpostingsHash */
	int dnfpostingsHashSize;
	int dnfpostingsHashHalfSize;
	int dnfpostingsHashMask;
	int dnffreePostingsCount;
	
	int begin();
	int textanalyze(char *data, int length, int fieldId);
	int tagsanalyze(char *data, int length, int fieldId);
	int dnfanalyze(char *data, int length, int fieldId);
	int end();
	
	void addTerms();
	void addDnfTerms();
	int termHash(TermInfo terminfo);
	bool postingEquals(char field, char* tokenText, int tokenTextLen);
	void morePostings();
	bool noNullPostings(RawPostingList** postings, int count);
	void termVInt(int i);
	void termByte(char b);
	void rehashPostings(int newSize);
	void dnfrehashPostings(int newSize);
	void dnftermByte(char b);
	void dnftermVInt(int i);
	bool dnfnoNullPostings(DnfRawPostingList** postings, int count);
	void dnfmorePostings();
	bool dnfpostingEquals(char field, char* tokenText, int tokenTextLen);
	void conjVInt(int off, int i);
	void conjByte(int off, char b);
	void saveIndex();
	void saveConjuction();
	int RawPostingListCmp(RawPostingList *p1, RawPostingList *p2);
	int DnfRawPostingListCmp(DnfRawPostingList *p1, DnfRawPostingList *p2);
	void compactPostings();
	void dnfcompactPostings();
	void sortposting(RawPostingList **postings, int lo, int hi);
	void sortdnfposting(DnfRawPostingList **postings, int lo, int hi);
	bool isEmpty(DnfRawPostingList *post);
	
};

#endif
