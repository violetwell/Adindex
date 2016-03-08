#ifndef QUERY_H
#define QUERY_H

#include <vector>
#include <string.h>
#include <set>

#include "Schema.h"
#include "SearchConfig.h"
#include "handler.h"
#include "QueryScore1.h"
#include "QueryScore2.h"
#include "thriftSearch.h"
#include "filter.h"

#define DNF_TAG_LEN 9
#define SIMI_TAG_LEN 9
#define FIELD_LEN 2
#define MAX_TAG_SIZE 60
#define TAG_LEN sizeof(int64_t)

#define DIFF_FIELD 0x00
#define SAME_FIELD 0x01


typedef struct dnf_q
{
	//char field[FIELD_LEN];
	int field;                             /* 4bytes, binary: dnfkey[0] dnfkey[1] 0 0 */
	char dnf_tag[TAG_LEN];                 /* dnf data(r_value+r_key) */
} DNF_Q;


class SearchQuery
{
private:
	QTerm _qterm;

	//dnf query
	char* _dnfquery;                       /* dnf queries, binary data: [key, value, dnftail]... */
	int _dnflen;                           /* dnf_count * DNF_TAG_LEN(9) */
	bool _dnfex;                           /* true if need no-dnf post list */

	//simi query
	char* _simiquery;                      /* tag queries, binary data:  [field_id r_tag(last 2 bytes reversed again)] */
	int _similen;                          /* simi_count * SIMI_TAG_LEN(9) */

	//filter query
	FilterInfo* _fquery;
	int _fsize;                            /* count of filter */

	//single query;
	QueryInfo* _singlequery;
	int _ssize;                            /* count of single query */

	bool _score2;                          /* true if contains tag query */
	bool _score1;                          /* true if contains single or dnf query */
	int _adsource;

	QueryScore1* scoreO;
	QueryScore2* scoreT;
public:
	SearchQuery(const QTerm& q);
	~SearchQuery();
	
	bool init(Schema* schema, SearchConfig* config);
	bool getScoreType2();
	bool getScoreType1();
	int getAdSource();
	int getDnfLen();
	DocIterator* score(IndexHandler* handler);
	DocIterator* score(MinHeap* hq, IndexHandler* handler);
	
};

#endif
