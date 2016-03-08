#ifndef __ADINDEX_H_
#define __ADINDEX_H_

#include "def.h"
#include "tinyxml.h"
#include "fields.h"
#include "docparser.h"
#include "analyzer.h"
#include "pool.h"
#include "store.h"
#include "inverted.h"
#include "realtime.h"
#include "merge.h"
#include "loadconf.h"
#include "handler.h"
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <stdint.h>
#include <pthread.h>
#include <vector>
#include <sys/time.h>
#include <unistd.h>
#include <sys/time.h>

#include "indexlog_types.h"
#include "InnerIndex.h"
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

#define FULL_MODE "full"
#define REALTIME_MODE "realtime"

typedef struct _Conf_t {
	
	//field schema
	IndexSchema *schema;
	//data parser
	DocParser *parser;
	//topic for message
	char* topic;
	//InnerIndexClient *client;
	long messageId;
	int addupdateCounter;                                  /* times of add/update doc operations in realtime-index */
	//start mode : full/realtime
	char *mode;
	//full index buffer
	int indexmaxdoc;
	//max doc per store file
	int storemaxdoc;
	//schema file path
	char *schemafile;
	//full source rawfile path
	char *fullsource;
	//full temp index path
	char *index;                                            /* temporary dir for parsing raw doc file when starting fullindex */
	//merge path
	char *merger;                                           /* temporary dir for merging realtime with incr */
	//full index path
	char *fullindex;                                        /* store data of fullindex */
	//incr index path
	char *incrindex;
	//realtime index path
	char *realtimeindex;
	//temp index path
	char *tempindex;
	//full leveldb path
	char *fulldict;
	//incr leveldb path
	char *incrdict;
	//realtime leveldb path
	char *realtimedict;
	//temp leveldb path
	char *tempdict;
	//inner index service thrift ip
	char *innerindexip;
	//inner index service thrift port
	int innerindexport;
	//incr message id store path
	char *incridfile;
	//full del reference
	DelRef *fullDelRef;
	//incr del reference
	DelRef *incrDelRef;
	
} Conf_t;

#endif
