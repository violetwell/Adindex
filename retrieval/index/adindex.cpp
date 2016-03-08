#include "adindex.h"
#include <log4cxx/logger.h>
#include <log4cxx/logstring.h>
#include <log4cxx/propertyconfigurator.h>

using namespace log4cxx;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;

Conf_t conf;
LoadConf adconf;
IndexHandler *handlers;
LoggerPtr indexLog(Logger::getLogger("adindex"));

int loadindexconf();
int loadschema();
int environment();
void *startindex(void *arg);

int main() {
	startindex(NULL);
	return 0;
}

void *startindex(void *arg) {
	if (loadindexconf() <= 0) {
		LOG4CXX_INFO(indexLog, "load index config file error, adindex exit");
		exit(-1);
	}
    IndexSchema *schema = new IndexSchema();
	conf.schema = schema;
	int res = 0;
	if (loadschema() <= 0) {
		LOG4CXX_INFO(indexLog, "load index schema file error, adindex exit");
		exit(-1);
	}
	if (environment() <= 0) {
		LOG4CXX_INFO(indexLog, "init environment error, adindex exit");
		exit(-1);
	}
	if (strcmp(conf.mode, REALTIME_MODE) == 0) {
		LOG4CXX_INFO(indexLog, "start adindex with realtime model");
		char tpfile[MAX_DIR_LEN];
		int i = 0;
		conf.fullDelRef = NULL;
		conf.incrDelRef = NULL;
		conf.addupdateCounter = 0;
		string topicstr(conf.topic);
		std::vector<InnerDocument> docs;
		IndexField *keyField = schema->getPrimary();
		boost::shared_ptr<TTransport> transport;
		InnerIndexClient *client = NULL;
		bool nodata = false;
		int cusumedDoc = 0;
		int res = 0;
		handlers = new IndexHandler(NULL, NULL, NULL);
		Handler *fullHandler = new Handler(conf.fullindex, schema);
		if (fullHandler->init() <= 0) {
			LOG4CXX_INFO(indexLog, "full index handler init fail");
			delete fullHandler;
			fullHandler = NULL;
		} else {
			char *idbrife = (char*)fullHandler->Brifes[keyField->getNumber()];
			int num = fullHandler->docnum;
			long *ids = (long*)malloc(sizeof(long) * num);
			for (i = 0; i < num; i++) {
				int ch1 = (int)(idbrife[i * 4] & 0xff);
				int ch2 = (int)(idbrife[i * 4 + 1] & 0xff);
				int ch3 = (int)(idbrife[i * 4 + 2] & 0xff);
				int ch4 = (int)(idbrife[i * 4 + 3] & 0xff);
				ids[i] = (long)(ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
			}
			DocIDMapper *mapper = new DocIDMapper(ids, num);
			DelRef *ref = (DelRef*)malloc(sizeof(DelRef));
			ref->mapper = mapper;
			ref->bitdata = (char*)fullHandler->DelDat;
			ref->length = fullHandler->DelDatSize;
			char *delFile = (char*)malloc(sizeof(char) * 1024);
			snprintf(delFile, 1024, "%s%s", conf.fullindex, INDEX_DEL);
			ref->filePath = delFile;
			conf.fullDelRef = ref;
			LOG4CXX_INFO(indexLog, "full index/mapper load success");
		}
	    handlers->setFullHandler(fullHandler);
		Handler *incrHandler = new Handler(conf.incrindex, schema);
		if (incrHandler->init() <= 0) {
			LOG4CXX_INFO(indexLog, "incr index handler init fail");
			delete incrHandler;
			incrHandler = NULL;
		} else {
			char *idbrife = (char*)incrHandler->Brifes[keyField->getNumber()];
			int num = incrHandler->docnum;
			long *ids = (long*)malloc(sizeof(long) * num);
			for (i = 0; i < num; i++) {
				int ch1 = (int)(idbrife[i * 4] & 0xff);
				int ch2 = (int)(idbrife[i * 4 + 1] & 0xff);
				int ch3 = (int)(idbrife[i * 4 + 2] & 0xff);
				int ch4 = (int)(idbrife[i * 4 + 3] & 0xff);
				ids[i] = (long)(ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
			}
			DocIDMapper *mapper = new DocIDMapper(ids, num);
			DelRef *ref = (DelRef*)malloc(sizeof(DelRef));
			ref->mapper = mapper;
			ref->bitdata = (char*)incrHandler->DelDat;
			ref->length = incrHandler->DelDatSize;
			char *delFile = (char*)malloc(sizeof(char) * 1024);
			snprintf(delFile, 1024, "%s%s", conf.incrindex, INDEX_DEL);
			ref->filePath = delFile;
			conf.incrDelRef = ref;
			LOG4CXX_INFO(indexLog, "incr index/mapper load success");
		}
	    handlers->setIncrHandler(incrHandler);
		Realtime *realtime = new Realtime(&conf);
		if (!realtime->ready()) {
			LOG4CXX_FATAL(indexLog, "init first realtime fail for leveldb dict");
			realtime->close();
			delete realtime;
			realtime = NULL;
		}
		handlers->setRealtimeHandler(realtime);
		while (1) {
			if (nodata) sleep(1);
			if (realtime == NULL) {
				sleep(10);
				continue;
			}
			docs.clear();
			try {
				if (client == NULL) {
				    boost::shared_ptr<TSocket> socket(new TSocket(conf.innerindexip, conf.innerindexport));
				    boost::shared_ptr<TTransport> newtransport(new TBufferedTransport(socket));
				    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(newtransport));
				    socket->setRecvTimeout(6000);
				    socket->setConnTimeout(6000);
					transport = newtransport;
				    transport->open();
					LOG4CXX_INFO(indexLog, "commitlog service open success");
					client = new InnerIndexClient(protocol);
				}
				client->loadData(docs, topicstr, conf.messageId, 10);
				if (docs.size() > 0) {
					LOG4CXX_INFO(indexLog, "load doc from commitlog num: " << docs.size());
				}
				if (docs.size() > 0) {
					nodata = false;
					int i = 0;
					for (i = 0; i < (int)docs.size(); i++) {
						InnerDocument document = docs[i];
						long id = document.id;
						conf.messageId = id + 1;
						string data = document.data;
						int dataLen =  document.dataLen;
						if (dataLen < 7) {
							LOG4CXX_WARN(indexLog, "doc data length less then 7, abandon this message, id:"<<id);
							continue;
						}
						char *dataptr = (char*)data.c_str();
						if (dataptr[0] == 0) {
							if (dataLen != 7) {
								LOG4CXX_WARN(indexLog, "doc data length not 7 for delete, abandon this message, id:"<<id);
								continue;
							}
							if (dataptr[1] != 126 || dataptr[6] != 127) {
								LOG4CXX_WARN(indexLog, "doc data not standard format for delete, abandon this message, id:"<<id);
								continue;
							}
							int ch1 = (int)(dataptr[2] & 0xff);
							int ch2 = (int)(dataptr[3] & 0xff);
							int ch3 = (int)(dataptr[4] & 0xff);
							int ch4 = (int)(dataptr[5] & 0xff);
							if ((ch1 | ch2 | ch3 | ch4) < 0) {
								LOG4CXX_WARN(indexLog, "doc data not standard format for delete, abandon this message, id:"<<id);
								continue;
							}
							int uid = (ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
							res = realtime->deleteDoc(uid);
							if (res <= 0) {
								LOG4CXX_WARN(indexLog, "delete doc fail: "<<uid);
							} else {
								LOG4CXX_INFO(indexLog, "delete doc success: "<<uid);
							}
						} else if (dataptr[0] == 1) {
							if (dataptr[1] != 126 || dataptr[dataLen - 1] != 127) {
								LOG4CXX_WARN(indexLog, "doc data not standard format for delete, abandon this message, id:"<<id);
								continue;
							}
							res = realtime->addDoc(dataptr + 1, dataLen - 1);
							if (res <= 0) {
								LOG4CXX_FATAL(indexLog, "add/update doc fail, id:"<<id);
								continue;
							}
							LOG4CXX_INFO(indexLog, "add/update doc success, id:"<<id);
							cusumedDoc++;
							conf.addupdateCounter++;
						} else {
							LOG4CXX_WARN(indexLog, "doc data not standard format for delete/update, abandon this message, id:"<<id);
							continue;
						}
					}
				} else {
					nodata = true;
					continue;
				}
				LOG4CXX_INFO(indexLog, "message id consumed: "<<(conf.messageId - 1));
				if (cusumedDoc >= 5000) {
					cusumedDoc = 0;
					Handler *newincrHandler = NULL;
					bool flag = true;
					snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.realtimeindex, "*");
					Utils::rmdirectory(tpfile);
					int res = realtime->close();
					if (res <= 0) {
						flag = false;
						LOG4CXX_FATAL(indexLog, "realtime index store fail");
					} else {
						if (handlers->incrHandler() == NULL) {
							snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.incrindex, "*");
							Utils::rmdirectory(tpfile);
							snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.incrdict, "*");
							Utils::rmdirectory(tpfile);
							Utils::copydirectory(conf.realtimeindex, conf.incrindex);
							Utils::copydirectory(conf.realtimedict, conf.incrdict);
							newincrHandler = new Handler(conf.incrindex, schema);
							if (newincrHandler->init() <= 0) {
								LOG4CXX_FATAL(indexLog, "realtime flush to disk reload fail");
								delete newincrHandler;
								newincrHandler = NULL;
								flag = false;
							}
						} else {
							snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.merger, "*");
							Utils::rmdirectory(tpfile);
							IncrMerger *merger = new IncrMerger(&conf, conf.merger, true, 
							conf.incrindex, conf.realtimeindex, conf.incrdict, conf.realtimedict);
							if (merger->merge() > 0) {
								LOG4CXX_INFO(indexLog, "incr merge success");
								snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.incrindex, "*");
								Utils::rmdirectory(tpfile);
								Utils::copydirectory(conf.merger, conf.incrindex);
								newincrHandler = new Handler(conf.incrindex, schema);
								if (newincrHandler->init() <= 0) {
									LOG4CXX_FATAL(indexLog, "incr merge flush to disk reload fail");
									delete newincrHandler;
									newincrHandler = NULL;
									flag = false;
								}
							} else {
								LOG4CXX_FATAL(indexLog, "incr merge fail");
								flag = false;
							}
							delete merger;
						}
					}
					if (flag) {
						Handler *oldIncr = handlers->incrHandler();
						Realtime *oldRealtime = handlers->realtimeHandler();
						DelRef *oldIncrRef = conf.incrDelRef;
						IndexHandler *oldHandlers = handlers;
						IndexHandler *temphandlers = new IndexHandler(handlers->fullHandler(), newincrHandler, NULL);
						handlers = temphandlers;
						LOG4CXX_INFO(indexLog, "index view changed without realtime");
						sleep(180);
						if (oldIncrRef != NULL) {
							char *filePath = oldIncrRef->filePath;
							DocIDMapper *mapper = oldIncrRef->mapper;
							if (filePath != NULL) free(filePath);
							if (mapper != NULL) delete mapper;
							free(oldIncrRef);
						}
						if (oldRealtime != NULL) delete oldRealtime;
						if (oldIncr != NULL) delete oldIncr;
						delete oldHandlers;
						char *idbrife = (char*)newincrHandler->Brifes[keyField->getNumber()];
						int num = newincrHandler->docnum;
						long *ids = (long*)malloc(sizeof(long) * num);
						for (i = 0; i < num; i++) {
							int ch1 = (int)(idbrife[i * 4] & 0xff);
							int ch2 = (int)(idbrife[i * 4 + 1] & 0xff);
							int ch3 = (int)(idbrife[i * 4 + 2] & 0xff);
							int ch4 = (int)(idbrife[i * 4 + 3] & 0xff);
							ids[i] = (long)(ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
						}
						DocIDMapper *mapper = new DocIDMapper(ids, num);
						DelRef *ref = (DelRef*)malloc(sizeof(DelRef));
						ref->mapper = mapper;
						ref->bitdata = (char*)newincrHandler->DelDat;
						ref->length = newincrHandler->DelDatSize;
						char *delFile = (char*)malloc(sizeof(char) * 1024);
						snprintf(delFile, 1024, "%s%s", conf.incrindex, INDEX_DEL);
						ref->filePath = delFile;
						conf.incrDelRef = ref;
						realtime = new Realtime(&conf);
						if (!realtime->ready()) {
							LOG4CXX_FATAL(indexLog, "init realtime fail for leveldb dict");
							realtime->close();
							delete realtime;
							realtime = NULL;
						}
						handlers->setRealtimeHandler(realtime);
						LOG4CXX_INFO(indexLog, "index view changed with new realtime @messageid: "<<conf.messageId);
						FILE *idFp = fopen(conf.incridfile, "w");
						snprintf(tpfile, MAX_DIR_LEN, "%ld\n", conf.messageId);
						fwrite(tpfile, sizeof(char), strlen(tpfile), idFp);
						fclose(idFp);
					} else {
						if (newincrHandler != NULL) {
							delete newincrHandler;
						}
						realtime = NULL;
						LOG4CXX_FATAL(indexLog, "realtime exchange fail");
					}
				}
			} catch (TException &tx) {
				nodata = true;
				if (transport != NULL) {
					try {
					    transport->close();
					} catch (TException &ctx) {
					}
				}
				if (client != NULL) {
					delete client;
					client = NULL;
				}
				LOG4CXX_FATAL(indexLog, "commitlog thrift service exception:");
			}
		}
	} else {
		IndexField *keyField = schema->getPrimary();
		DocParser *parser = new DocParser(0, conf.fullsource, schema);
		conf.parser = parser;
		Brife *brife = new Brife(&conf);
		Store *store = new Store(&conf);
		Inverted *inverted = new Inverted(&conf);
		int docid = 0;
		while (parser->hasNext()) {
			char *key = parser->data(keyField->getNumber());
			if (key == NULL) continue;
			int res = inverted->index(parser);
			if (res <= 0) continue;
			res = brife->saveDoc(parser);
			res = store->saveDoc(parser);
			LOG4CXX_INFO(indexLog, "index doc: "<<docid++);
		}
		brife->close();
		store->close();
		inverted->close();
		delete brife;
		delete store;
		delete inverted;
		delete parser;
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
		free(ids);
		delete merger;
	}
	delete schema;
    return ((void *)0);
}

int loadindexconf() {
	if (adconf.load_conf("adindexconf.txt") <= 0) {
		LOG4CXX_INFO(indexLog, "can't load config file");
		return -1;
	}
	int indexmaxdoc = adconf.get_int("indexmaxdoc");
	if (indexmaxdoc < 10) return -1;
	LOG4CXX_INFO(indexLog, "full index max doc: "<<indexmaxdoc);
	conf.indexmaxdoc = indexmaxdoc;
	int storemaxdoc = adconf.get_int("storemaxdoc");
	if (storemaxdoc < 1000000) return -1;
	LOG4CXX_INFO(indexLog, "index max store doc: "<<storemaxdoc);
	conf.storemaxdoc = storemaxdoc;
	char *schemafile = adconf.get_str("schemafile");
	if (schemafile == NULL) return -1;
	LOG4CXX_INFO(indexLog, "schema file path: "<<schemafile);
	conf.schemafile = schemafile;
	char *fullsource = adconf.get_str("fullsource");
	if (fullsource == NULL) return -1;
	LOG4CXX_INFO(indexLog, "full index source path: "<<fullsource);
	conf.fullsource = fullsource;
	char *index = adconf.get_str("index");
	if (index == NULL) return -1;
	LOG4CXX_INFO(indexLog, "temp index file path: "<<index);
	conf.index = index;
	char *merge = adconf.get_str("merge");
	if (merge == NULL) return -1;
	LOG4CXX_INFO(indexLog, "temp merge file path: "<<merge);
	conf.merger = merge;
	char *fullindex = adconf.get_str("fullindex");
	if (fullindex == NULL) return -1;
	LOG4CXX_INFO(indexLog, "full index file path: "<<fullindex);
	conf.fullindex = fullindex;
	char *incrindex = adconf.get_str("incrindex");
	if (incrindex == NULL) return -1;
	LOG4CXX_INFO(indexLog, "incr index file path: "<<incrindex);
	conf.incrindex = incrindex;
	char *realtimeindex = adconf.get_str("realtimeindex");
	if (realtimeindex == NULL) return -1;
	LOG4CXX_INFO(indexLog, "realtime index file path: "<<realtimeindex);
	conf.realtimeindex = realtimeindex;
	char *fulldict = adconf.get_str("fulldict");
	if (fulldict == NULL) return -1;
	LOG4CXX_INFO(indexLog, "full dict path: "<<fulldict);
	conf.fulldict = fulldict;
	char *incrdict = adconf.get_str("incrdict");
	if (incrdict == NULL) return -1;
	LOG4CXX_INFO(indexLog, "incr dict path: "<<incrdict);
	conf.incrdict = incrdict;
	char *realtimedict = adconf.get_str("realtimedict");
	if (realtimedict == NULL) return -1;
	LOG4CXX_INFO(indexLog, "realtime dict path: "<<realtimedict);
	conf.realtimedict = realtimedict;
	char *tempdict = adconf.get_str("tempdict");
	if (tempdict == NULL) return -1;
	LOG4CXX_INFO(indexLog, "temp dict path: "<<tempdict);
	conf.tempdict = tempdict;
	char *innerindexip = adconf.get_str("innerindexip");
	if (innerindexip == NULL) return -1;
	LOG4CXX_INFO(indexLog, "inner index service ip: "<<innerindexip);
	conf.innerindexip = innerindexip;
	int innerindexport = adconf.get_int("innerindexport");
	if (innerindexport <= 1024) return -1;
	LOG4CXX_INFO(indexLog, "inner index service port: "<<innerindexport);
	conf.innerindexport = innerindexport;
	char *mode = adconf.get_str("mode");
	if (mode == NULL) return -1;
	LOG4CXX_INFO(indexLog, "adindex start mode: "<<mode);
	conf.mode = mode;
	char *incridfile = adconf.get_str("incridfile");
	if (incridfile == NULL) return -1;
	LOG4CXX_INFO(indexLog, "incr message id path: "<<incridfile);
	conf.incridfile = incridfile;
	char *topic = adconf.get_str("topic");
	if (topic == NULL) return -1;
	LOG4CXX_INFO(indexLog, "adindex start topic: "<<topic);
	conf.topic = topic;
	return 1;
}

int loadschema() {
	IndexSchema *schema = conf.schema;
	TiXmlDocument *doc = new TiXmlDocument(conf.schemafile);
    bool loadOkay = doc->LoadFile();
    if (!loadOkay) {
		LOG4CXX_INFO(indexLog, "can't load schema file");
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
		LOG4CXX_INFO(indexLog, "realtime index path has been cleared");
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.realtimedict, "*");
		Utils::rmdirectory(tpfile);
		LOG4CXX_INFO(indexLog, "realtime index leveldb data path has been cleared");
		if (Utils::existFile(conf.incridfile)) {
			FILE *idFp = fopen(conf.incridfile, "r");
			if (fgets(tpfile, MAX_DIR_LEN, idFp) == NULL) {
				fclose(idFp);
				LOG4CXX_INFO(indexLog, "incrid conf file can't be opened");
				return -1;
			} else {
				fclose(idFp);
				conf.messageId = std::atol(tpfile);
				if (conf.messageId < 0) {
					LOG4CXX_INFO(indexLog, "incrid less then zero");
					return -1;
				}
			}
		} else {
			FILE *idFp = fopen(conf.incridfile, "w");
			if (idFp == NULL) {
				LOG4CXX_INFO(indexLog, "incrid conf file can't be opened");
				return -1;
			}
			snprintf(tpfile, MAX_DIR_LEN, "%ld\n", 0l);
			int res = fwrite(tpfile, sizeof(char), strlen(tpfile), idFp);
			if (res != (int)strlen(tpfile)) {
				LOG4CXX_INFO(indexLog, "incrid conf file can't be writted");
				return -1;
			}
			fclose(idFp);
		}
		boost::shared_ptr<TSocket> socket(new TSocket(conf.innerindexip, conf.innerindexport));
		boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
		boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
		socket->setRecvTimeout(6000);
		socket->setConnTimeout(6000);
		try {
			transport->open();
			LOG4CXX_INFO(indexLog, "commitlog service for test open ok");
			transport->close();
			LOG4CXX_INFO(indexLog, "commitlog service for test closed");
		} catch (TException &tx) {
			LOG4CXX_INFO(indexLog, "commitlog service for test open fail, start adindex fail");
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
		LOG4CXX_INFO(indexLog, "full index path(temp, merge, index) has been cleared");
		snprintf(tpfile, MAX_DIR_LEN, "%s%s", conf.fulldict, "*");
		Utils::rmdirectory(tpfile);
		LOG4CXX_INFO(indexLog, "full index leveldb data path has been cleared");
		return 1;
	} else {
	    LOG4CXX_INFO(indexLog, "unavailable start mode(full/realtime)");
	    return -1;
	}
}
