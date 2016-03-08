#include <iostream>
#include <fstream>
#include <cstdlib>
#include <unistd.h>
#include <string>
#include <map>
#include "time.h"

#include "pthread.h"

#include "Schema.h"
#include "docparser.h"
#include "fields.h"
#include "tinyxml.h"

#include "Index.h"
#include <config.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using boost::shared_ptr;

using namespace std;
map<int, map<string, string> > docl;
IndexSchema* schema = new IndexSchema();

int loadschema() {
	//IndexSchema *schema = conf.schema;
	TiXmlDocument *doc = new TiXmlDocument("/opt/wk/retrieval/config/schema.xml");
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

bool load_rawfile()
{
	ifstream fin;
	fin.open("rawfile.0.0");

	//fin >> 
	if( loadschema() <= 0)
	{
		printf("load schema conf error\n");
		exit(-1);
	}

	DocParser* parser = new DocParser(0, "/opt/wk/retrieval/test/", schema);

	string k_path = "/opt/wk/retrieval/config/schema.xml";
	Schema* kschema = new Schema(k_path);
	if(kschema->init())
	{
		cout << "schema init ok" << endl;
	}
	else
	{
		exit(1);
	}

	vector<unsigned char> allFid;
	allFid = kschema->getAllFid();
	vector<string> allFname;
	allFname = kschema->getAllFname();

	int count(0);
	string id("id");
	while(parser->hasNext())
	{
		count++;
		int docid;
		map<string, string> doc;
		for(int i(0); i < allFname.size(); i++)
		{
			string key, val;
			key = allFname[i];
			char* valbuf = parser->data(allFid[i]);
			int length = parser->length(allFid[i]);
			val.assign((const char *)valbuf, length);
			if(key == id)
			{
				docid = *(int*)valbuf;
			}
			doc[key] = val;
		}
		docl[docid] = doc;
		if(!(count%10000))
		{
			cout << "load doc: " << count << endl;
		}
		//cout << count << " " << docid << endl;
	}
}



int main(int argc, char** argv)
{
	//init paras
	int step, israndom, op, uid;
	string topic("content");
	Document doc;

	load_rawfile();

    boost::shared_ptr<TSocket> socket(new TSocket("10.10.92.32", 9804));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

    transport->open();
    IndexClient client(protocol);
	int committype(0);
	map<int, map<string, string> >::iterator dl_it;
	dl_it = docl.begin();
	srand((unsigned)time( NULL ));
	while(1)
	{
		cout << "please input step" << endl;
		cin >> step;
		if(step < 0)
		{
			cout << "Inviliad Paramater" << endl;
			exit(1);
		}
		cout << "please input israndom" << endl;
		cin >> israndom;
		if(israndom < 0 || israndom > 1)
		{
			cout << "Inviliad Paramater" << endl;
			exit(1);
		}
		else if(0 == israndom)
		{
			cout << "please input opration" << endl;
			cin >> op;
			if(0 == op/3)
			{
				cout << "please input uid" << endl;
				cin >> uid;
			}
		}
		for(int j(0); j < step; j++)
		{
			if(0 == israndom)
			{
				committype = op;
			}
			else
			{
				committype = rand() % 6;
				uid = dl_it->first;
			}

			doc.__set_fields(dl_it->second);
			if(0 ==  committype/3)
			{
				client.deleteDoc(topic, uid);
			}
			else if(1 == committype/3)
			{
				client.addDoc(topic, doc);
			}
			else
			{
				client.updateDoc(topic, doc);
			}
		}
		dl_it++;

		/*
		cout << docl.size() << endl;
		int doc_id;
		cin >> doc_id;
		map<string, string> doc;
		doc = docl[doc_id];
		map<string, string>::iterator it;
		for(it = doc.begin(); it != doc.end(); it++)
		{
			cout << it->first << ":" << it->second << endl;
		}
		*/
	}
	return 0;


    transport->close();
}
