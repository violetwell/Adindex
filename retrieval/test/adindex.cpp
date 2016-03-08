#include "adindex.h"

#include <unistd.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;

Conf_t conf;
LoadConf adconf;
IndexHandler *handlers;

int loadindexconf();
int loadschema();
int environment();

int main() {
	if (loadindexconf() <= 0) {
		printf("load index conf error\n");
		exit(-1);
	}
	printf("loaded index conf\n");
    IndexSchema *schema = new IndexSchema();
	conf.schema = schema;
	int res = 0;
	if (loadschema() <= 0) {
		printf("load schema conf error\n");
		exit(-1);
	}
/*	Handler *fullhandler = new Handler(conf.fullindex, schema);
	if (fullhandler->init() > 0) {
		printf("load full index handler success\n");
	}
	delete fullhandler;
	Handler *incrhandler = new Handler(conf.merger, schema);
	if (incrhandler->init() > 0) {
		printf("load incr index handler success\n");
	}
	delete incrhandler;
	//-----
	printf("loaded schema conf\n");
	conf.realtimedict = "/opt/hhindex/leveldb/realtime/";
	conf.realtimeindex = "/opt/hhindex/data/index/realtime/";
	Realtime *realtime = new Realtime(&conf);
	int i = 0, begin = 0;
	FILE *file = NULL;
	file = fopen("/opt/hhindex/data/source/rawfile.0.0", "r");
	BufferedReader *br = new BufferedReader(file, 0);
	char *buffer = (char*)malloc(10240000);
	memset(buffer, 0, 10240000);
	int read = br->read(buffer, 0, 10240000);
	int index = 0;
	bool doRead = false;
	while (true) {
		doRead = true;
		for (i = begin; i < read; i++) {
			if (buffer[i] == 127 && (buffer[i + 1] == 126 || i == (read - 1))) {
				doRead = false;
				break;
			}
		}
		i++;
		if (doRead) break;
		realtime->addDoc(buffer + begin, i - begin);
		begin = i;
		printf("docid: %d\n", index);
		index++;
	}
	res = realtime->close();
	if (res <= 0) {
		printf("realtime index store fail\n");
	}
	free(buffer);
	delete br;
	delete realtime;*/
	//-----
	system("rm -f /opt/hhindex/leveldb/full/*");
	system("rm -f /opt/hhindex/data/index/full/*");
	system("rm -f /opt/hhindex/data/index/index/*");
	int docid = 0;
	IndexField *keyField = schema->getPrimary();
	DocParser *parser = new DocParser(0, conf.fullsource, schema);
	conf.parser = parser;
	Brife *brife = new Brife(&conf);
	Store *store = new Store(&conf);
	Inverted *inverted = new Inverted(&conf);
	while (parser->hasNext()) {
		char *key = parser->data(keyField->getNumber());
		if (key == NULL) continue;
		res = inverted->index(parser);
		if (res <= 0) continue;
		res = brife->saveDoc(parser);
		res = store->saveDoc(parser);
		printf("index doc:%d\n", docid++);
	}
	brife->close();
	store->close();
	
	inverted->close();
	
	int groupnum = (docid + conf.indexmaxdoc - 1) / conf.indexmaxdoc;
	int *ids = (int*)malloc(sizeof(int) * groupnum);
	int i = 0;
	for (i = 0; i < groupnum; i++) ids[i] = i;
	FullMerger *merger = new FullMerger(&conf, conf.fullindex, true, conf.index, conf.index, ids, groupnum, -1);
	if (merger->merge() <= 0) {
		res = 0;
	} else {
		res = 1;
	}
	delete merger;
	
	free(ids);

	//-----
	delete brife;
	delete store;
	delete inverted;

	delete parser;
	delete schema;
	return 0;
}


int loadindexconf() {
	if (adconf.load_conf("adindexconf.txt") <= 0) {
		return -1;
	}
	int indexmaxdoc = adconf.get_int("indexmaxdoc");
	if (indexmaxdoc < 100) return -1;
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

int loadschema() {
	IndexSchema *schema = conf.schema;
	TiXmlDocument *doc = new TiXmlDocument(conf.schemafile);
    bool loadOkay = doc->LoadFile();
    if (!loadOkay) {
		printf("can't load schema file, error='%s'\n", doc->ErrorDesc());
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
		count++;
    }
    TiXmlElement *id = fields->NextSiblingElement();
	if (NULL == id) return 0;
	else {
		const char *value = id->Attribute(INDEX_SCHEMA_KEY);
		if (value == NULL || strlen(value) == 0) return 0;
		else {
			string s(value);
			IndexField *get = schema->getField(s);
			if (get == NULL) return 0;
			else {
				schema->setPrimary(get);
			}
		}
	}
	free(tmp);
	return 1;
}

int environment() {
	char tpfile[MAX_DIR_LEN];
	if (strcmp(conf.mode, REALTIME_MODE) == 0) {
		conf.messageId = 0;
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.realtimeindex, "*");
		Utils::rmdirectory(tpfile);
		printf("realtime index path has been cleared\n");
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.realtimedict, "*");
		Utils::rmdirectory(tpfile);
		printf("realtime index leveldb data path has been cleared\n");
		if (Utils::existFile(conf.incridfile)) {
			FILE *idFp = fopen(conf.incridfile, "r");
			if (fgets(tpfile, MAX_DIR_LEN, idFp) == NULL) {
				fclose(idFp);
				return -1;
			} else {
				fclose(idFp);
				conf.messageId = std::atol(tpfile);
				if (conf.messageId < 0) return -1;
			}
		} else {
			FILE *idFp = fopen(conf.incridfile, "w");
			if (idFp == NULL) return -1;
			snprintf(tpfile, MAX_DIR_LEN, "%ld", 0l);
			int res = fwrite(tpfile, sizeof(char), strlen(tpfile), idFp);
			if (res != (int)strlen(tpfile)) return -1;
			fclose(idFp);
		}
		boost::shared_ptr<TSocket> socket(new TSocket(conf.innerindexip, conf.innerindexport));
		boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
		boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
		socket->setRecvTimeout(1000);
		socket->setConnTimeout(1000);
		try {
			transport->open();
			transport->close();
		} catch (TException &tx) {
			return -1;
		}
		return 1;
	} else if (strcmp(conf.mode, FULL_MODE) == 0) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.index, "*");
		Utils::rmdirectory(tpfile);
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.merger, "*");
		Utils::rmdirectory(tpfile);
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.fullindex, "*");
		Utils::rmdirectory(tpfile);
		printf("full index path(temp, merge, index) has been cleared\n");
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.fulldict, "*");
		Utils::rmdirectory(tpfile);
		printf("full index leveldb data path has been cleared\n");
		return 1;
	}
	printf("unavailable start mode(full/realtime)\n");
	return -1;
}
