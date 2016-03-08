#ifndef  __STORE_H_
#define  __STORE_H_

#include "def.h"
#include "adindex.h"

class Brife {
	
public:
	Brife(void *_conf);
	~Brife();
	int close();
	int saveDoc(DocParser *parser);
	
private:
	IndexSchema *schema;
	char *path;
	int maxdoc;
	int fileNumber;
	int docNumber;
	int brifeFieldNumber;
	IndexField **brifeField;                  /* store the brife IndexField in schema */
	char **dataBuffer;                        /* dataBuffer[i] store the data of ith briefField of all doc */
	char **dataName;                          /* dataName[i] store the name of ith briefField */
	FILE **brifeFiles;
	
	int init();
	int writeData();
	
};

class Store {
	
public:
	Store(void *_conf);
	~Store();
	int close();
	int saveDoc(DocParser *parser);
	
private:
	IndexSchema *schema;
	char *path;
	int maxdoc;
	int fileNumber;
	int docNumber;
	Store_Pos *posArr;
	Store_Length *lenArr;
	FILE *storeFile;
	int storeFieldNumber;
	IndexField **storeField;                   /* store the store IndexField in schema*/
	char *storeBuffer;                         /* store the data of store_data of all doc */
	int storeLen;
	
	int writeData();
	int init();
	int savePosLen();
	
};

#endif
