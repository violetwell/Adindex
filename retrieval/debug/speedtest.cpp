#include "QueryScore2.h"
#include "MinHeap.h"
#include "DocInfo.h"
#include <sys/time.h>
#include "fields.h"
#include "loadconf.h"

class SearchStore;
LoadConf adconf;
Conf_t conf;
struct timeval rtbeg;

int loadschema(IndexSchema *schema );
int loadindexconf(IndexSchema *schema );

int main()
{
    int ret;
	IndexSchema* schema = new IndexSchema();
	ret = loadschema(schema);
	if (ret == 0)
	{
		cout<<"loadschema error" <<endl;
		return -1;
	}
	ret = loadindexconf(schema);
	if (ret < 0)
	{
		cout<<"loadindexconf error" <<endl;
		return -1;
	}

	//Handler *fullhandler = new Handler("/opt/wdx/data/index/full/", schema);
	//  Handler *fullhandler = new Handler("/opt/hhindex/data/index/full/", schema);
	//Handler *incrhandler = new Handler("/opt/wdx/data/index/incr/", schema);
    //Handler *incrhandler = new Handler("/opt/hhindex/data/index/base/", schema);
    Handler *incrhandler = new Handler("/opt/hhindex/data/index/incr/", schema);

    ret = incrhandler->init();
	if (ret == 0)
	{
	    cout << "incrhandler init failed" << endl;
	    return -1;
    }
	IndexHandler *handle = new IndexHandler(NULL, incrhandler, NULL);

	int dnflen = 4;
    char *dnftemp = (char*)malloc(7*dnflen);
    long long dnfdata[4] ={0};
    dnfdata[0] = 281475170909184;
    dnfdata[1] = 281475171040256;
    dnfdata[2] = 281475205250048;
    dnfdata[3] = 281475272686592;
															
    int idxdnf=0;
    for(int i = 0; i <dnflen; i++){
        int shift = 8;
        for(int j = 0; j<6; j++) {
            dnftemp[idxdnf+5-j] = (dnfdata[i] >> shift *(j+2)) & 0x00ff;
        }
		idxdnf += 7;
	}

    idxdnf = 7;
    for (int i = 0; i <dnflen; i++) {
        if (i == 0) dnftemp[idxdnf-1] = 0;
        else dnftemp[idxdnf * (i+1) -1] = 1;
    }
    int num = 7 * dnflen;


	int taglen = 13;
    long long tagdata[13] ={0};

    tagdata[0] = 281475170909184;
    tagdata[1] = 281475171040256;
    tagdata[2] = 281475205250048;
    tagdata[3] = 281475272686592;
    tagdata[4] = 1193582049506830;
    tagdata[5] = 1223082950800862;
    tagdata[6] = 1229604850513670;
    tagdata[7] = 1248275847919131;
    tagdata[8] = 1470134356419806;
    tagdata[9] = 1475057026213416;
    tagdata[10] = 1511079827221102;
    tagdata[11] = 1525894920676869;
    tagdata[12] = 1531407956059831;


    char* temp = (char *)malloc(9*13);
    int idx = 0;
    int tnum = 4;
    int bnum = 5;
    int cnum = 4;
    for (int i = 0; i < cnum; i++){
        temp[idx++] = 3;
        int shift = 8;
        temp[idx+6] = (tagdata[i]) & 0x00ff;
        temp[idx+7] = (tagdata[i] >> shift) & 0x00ff;
        for (int j = 0; j < 6; j++) {
            temp[idx+5-j] = (tagdata[i] >> ((j+2) *shift)) & 0x00ff;
        }
        idx += sizeof(tagdata[i]);
    }
    for (int i = cnum; i < cnum + tnum; i++){
        temp[idx++] = 1;
        int shift = 8;
        temp[idx+6] = (tagdata[i]) & 0x00ff;
        temp[idx+7] = (tagdata[i] >> shift) & 0x00ff;
        for (int j = 0; j < 6; j++) {
            temp[idx+5-j] = (tagdata[i] >>( (j+2) *shift)) & 0x00ff;
        }
        idx += sizeof(tagdata[i]);
    }
    for (int i = cnum + tnum; i < bnum+cnum + tnum; i++){
        temp[idx++] = 2;
        int shift = 8;
        temp[idx+6] = (tagdata[i]) & 0x00ff;
        temp[idx+7] = (tagdata[i] >> shift) & 0x00ff;
        for (int j = 0; j < 6; j++) {
        temp[idx+5-j] = (tagdata[i] >> ((j+2) *shift)) & 0x00ff;
		}
		idx += sizeof(tagdata[i]);
	}


	struct timeval begTime, endTime;
    printf("time start********\n");
    gettimeofday(&begTime, NULL);
	bool dnfex = false;
	int ndocs = 50;
	int index = 0;
	MinHeap* hq = new MinHeap(ndocs);
	DnfDocIterator *dnf = new DnfDocIterator(handle, dnftemp, num, NULL);
	//QueryScore1 *score1 = new QueryScore1(dnftemp, num, dnfex, NULL, NULL, handle);
    QueryScore2 *tag = new QueryScore2(hq, dnf, temp, 13 * 9, handle, NULL);
    while(tag->next()) {
		ScoreDoc sd;
		sd.doc = tag->docId();
		sd.score = tag->score();
        index++;
        hq->insert(sd);
    }
    gettimeofday(&endTime, NULL);
    long  diff = 1000000 * (endTime.tv_sec-begTime.tv_sec) + endTime.tv_usec-begTime.tv_usec;
    diff = (endTime.tv_sec * 1000 * 1000 + endTime.tv_usec) - (begTime.tv_sec * 1000 * 1000 + begTime.tv_usec);
    printf("num = %d, diff = %ld\n", index, diff);
}


int loadschema(IndexSchema *schema ) {
    if (adconf.load_conf("adindexconf.txt") <= 0) {
        cout << "load_conf file fail" << endl;
        return -1;
    }
    char *schemafile = adconf.get_str("schemafile");
    TiXmlDocument *doc = new TiXmlDocument(schemafile);
    bool loadOkay = doc->LoadFile();
    if (!loadOkay){
        printf("can't load schema file, error='%s'\n", doc->ErrorDesc());
        delete doc;
        return 0;
    }
    TiXmlElement *root = doc->RootElement();
    TiXmlElement *fields = root->FirstChildElement();
    TiXmlElement *field = NULL;
    int count = 0, num = 0;
    char *tmp = (char*)malloc(1024);
    for (field = fields->FirstChildElement(); field; field = field->NextSiblingElement()) {
        num = 0;
        int length = -1;
        char *name, *type;
        bool indexed = false, stored = false, filter = false;
        const char *value = field->Attribute(INDEX_SCHEMA_NAME);
        if (value == NULL || strlen(value) == 0) return 0;
        strcpy(tmp + num, value);
        name = tmp + num;
        num += strlen(value);
        tmp[num++] = 0;
        value = field->Attribute(INDEX_SCHEMA_TYPE);
        if (value == NULL || strlen(value) == 0) return 0;
        strcpy(tmp + num, value);
        type = tmp + num;
        num += strlen(value);
        tmp[num++] = 0;
        value = field->Attribute(INDEX_SCHEMA_LENGTH);
        if (value != NULL || strlen(value) > 0) {
            length = atoi(value);
        }
        value = field->Attribute(INDEX_SCHEMA_INDEXED);
        if (value != NULL || strlen(value) > 0) {
            if (strcmp(value, "true") == 0) indexed = true;
        }
        value = field->Attribute(INDEX_SCHEMA_STORED);
        if (value != NULL || strlen(value) > 0) {
            if (strcmp(value, "true") == 0) stored = true;
        }
        value = field->Attribute(INDEX_SCHEMA_FILTER);
        if (value != NULL || strlen(value) > 0) {
            if (strcmp(value, "true") == 0) filter = true;
        }
        IndexField *field = new IndexField(name, count, type, length, indexed, stored, filter);
        schema->addField(*(new string(name)), field);
        //delete field;
        count++;
    }
    TiXmlElement *id = fields->NextSiblingElement();
    if (NULL == id) {
        delete doc;
        return 0;
    }
    else {
        const char *value = id->Attribute(INDEX_SCHEMA_KEY);
        if (value == NULL || strlen(value) == 0)
         {
             delete doc;
             return 0;
         }
        else {
            string s(value);
            IndexField *get = schema->getField(s);
            if (get == NULL) 
            {
                delete doc;
                return 0;
            }
            else {
                schema->setPrimary(get);
            }
        }
    }
    free(tmp);
    delete doc;
    return 1;
}

int loadindexconf(IndexSchema *schema) {
    conf.schema = schema;
    int indexmaxdoc = adconf.get_int("indexmaxdoc");
    if (indexmaxdoc < 10) return -1;
    printf("full index max doc: %d\n", indexmaxdoc);
    conf.indexmaxdoc = indexmaxdoc;
    int storemaxdoc = adconf.get_int("storemaxdoc");
    if (storemaxdoc < 1000000) return -1;
    printf("index max store doc: %d\n", storemaxdoc);
    conf.storemaxdoc = storemaxdoc;
    char *schemafile = adconf.get_str("schemafile");
    if (schemafile == NULL) return -1;
    printf("schema file path: %s\n", schemafile);
    conf.schemafile = schemafile;
    char *fullsource = adconf.get_str("fullsource");
    if (fullsource == NULL) return -1;
    printf("full index source path: %s\n", fullsource);
    conf.fullsource = fullsource;
    char *index = adconf.get_str("index");
    if (index == NULL) return -1;
    printf("temp index file path: %s\n", index);
    conf.index = index;
    char *merge = adconf.get_str("merge");
    if (merge == NULL) return -1;
    printf("temp merge file path: %s\n", merge);
    conf.merger = merge;
    char *fullindex = adconf.get_str("fullindex");
    if (fullindex == NULL) return -1;
    printf("full index file path: %s\n", fullindex);
    conf.fullindex = fullindex;
    char *incrindex = adconf.get_str("incrindex");
    if (incrindex == NULL) return -1;
    printf("incr index file path: %s\n", incrindex);
    conf.incrindex = incrindex;
    char *realtimeindex = adconf.get_str("realtimeindex");
    if (realtimeindex == NULL) return -1;
    printf("realtime index file path: %s\n", realtimeindex);
    conf.realtimeindex = realtimeindex;
    char *fulldict = adconf.get_str("fulldict");
    if (fulldict == NULL) return -1;
    printf("full dict path: %s\n", fulldict);
    conf.fulldict = fulldict;
    char *incrdict = adconf.get_str("incrdict");
    if (incrdict == NULL) return -1;
    printf("incr dict path: %s\n", incrdict);
    conf.incrdict = incrdict;
    char *realtimedict = adconf.get_str("realtimedict");
    if (realtimedict == NULL) return -1;
    printf("realtime dict path: %s\n", realtimedict);
    conf.realtimedict = realtimedict;
    char *tempdict = adconf.get_str("tempdict");
    if (tempdict == NULL) return -1;
    printf("temp dict path: %s\n", tempdict);
    conf.tempdict = tempdict;
    char *innerindexip = adconf.get_str("innerindexip");
    if (innerindexip == NULL) return -1;
    printf("inner index service ip: %s\n", innerindexip);
    conf.innerindexip = innerindexip;
    int innerindexport = adconf.get_int("innerindexport");
    if (innerindexport <= 1024) return -1;
    printf("inner index service port: %d\n", innerindexport);
    conf.innerindexport = innerindexport;
    char *mode = adconf.get_str("mode");
    if (mode == NULL) return -1;
    printf("adindex start mode: %s\n", mode);
    conf.mode = mode;
    char *incridfile = adconf.get_str("incridfile");
    if (incridfile == NULL) return -1;
    printf("incr message id path: %s\n", incridfile);
    conf.incridfile = incridfile;
    char *topic = adconf.get_str("topic");
    if (topic == NULL) return -1;
    printf("adindex start topic: %s\n", topic);
    conf.topic = topic;
    return 1;
}
