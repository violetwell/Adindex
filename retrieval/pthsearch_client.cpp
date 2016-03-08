#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include "time.h"

#include "thriftSearch.h"
#include <config.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TCompactProtocol.h>
#include <pthread.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using boost::shared_ptr;

long long qsum = 0;
pthread_mutex_t qsum_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct tags
{
	std::vector<int64_t> body;
	std::vector<int64_t> title;
	std::vector<int64_t> dnf;
	std::vector<int64_t> total;
} TAGS;

typedef struct args
{
	TAGS t;
	struct timeval tpstart;
} ARGS;

void load_tags(TAGS& t)
{
	std::string body_path = "/opt/retrieval-work/retrieval/debug/debugdata/body";
	std::string title_path = "/opt/retrieval-work/retrieval/debug/debugdata/title";
	std::string dnf_path = "/opt/retrieval-work/retrieval/debug/debugdata/dnf";
	std::string total_path = "/opt/retrieval-work/retrieval/debug/debugdata/total";

	std::ifstream fin;
	fin.open(body_path.c_str());
	if(!fin.is_open())
	{
		std::cout << "error can't open body" << std::endl;
		exit(1);
	}

	int64_t i64;
	while(fin >> i64)
	{
		t.body.push_back(i64);
	}
	std::cout << t.body.size() << std::endl;
	fin.close();

	fin.open(title_path.c_str());
	if(!fin.is_open())
	{
		std::cout << "error can't open title" << std::endl;
	}
	while(fin >> i64)
	{
		t.title.push_back(i64);
	}
	std::cout << t.title.size() << std::endl;
	fin.close();

	/*
	fin.open(dnf_path.c_str());
	if(!fin.is_open())
	{
		std::cout << "error can't open dnf" << std::endl;
	}
	while(fin >> i64)
	{
		t.dnf.push_back(i64);
	}
	std::cout << t.dnf.size() << std::endl;
	fin.close();
	*/

	fin.open(total_path.c_str());
	if(!fin.is_open())
	{
		std::cout << "error can't open total" << std::endl;
	}
	while(fin >> i64)
	{
		t.total.push_back(i64);
	}
	std::cout << t.total.size() << std::endl;
	fin.close();
	
}

void *work_thread(void *arg)
{
		//¼ÆÊ±
	ARGS ar = *(ARGS*)arg;
	struct timeval tpstart = ar.tpstart;
	struct timeval tpend;
	TAGS t = ar.t;
  double timeuse;
 	gettimeofday(&tpend, NULL);
	timeuse=1000000L*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;
	double runtime = 24*60*60*1000000L;
	
	boost::shared_ptr<TSocket> socket(new TSocket("10.10.68.64", 22535));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

	transport->open();
	thriftSearchClient client(protocol);
		
	while(timeuse <= runtime)
	{
		//single_query
		qpair s_query1;
		std::string qkey = "des";
		std::string qval = "00099900";
		s_query1.__set_qkey(qkey);
		s_query1.__set_qval(qval);
	
		qpair s_query2;
		qkey = "slotid";
		qval = "120_0";
		s_query2.__set_qkey(qkey);
		s_query2.__set_qval(qval);
	
		qpair s_query3;
		qkey = "size";
		//qval = "160240";
		qval = "9";
		s_query3.__set_qkey(qkey);
		s_query3.__set_qval(qval);
	
		std::vector<qpair> single_query;
		//single_query.push_back(s_query1);
		single_query.push_back(s_query2);
		single_query.push_back(s_query3);
	
		/*
		//dnf query
		std::vector<int64_t> dnf;
		srand((unsigned)time( NULL ));
		int dnf_size = t.dnf.size();
		int dnflen = rand()%4;
		for(int i(0); i < dnflen; i++)
		{
			int index = rand()%dnf_size;
			int64_t dval = t.dnf[index];//st020100
			dnf.push_back(dval);
		}
		*/
	
	
		//simi query
		std::vector<int64_t> tags;
		int total_size = t.total.size();
		int totallen = rand()%20;
		for(int i(0); i < 20; i++)
		{
			int index = rand()%total_size;
			int64_t val = t.total[index];
			tags.push_back(val);
		}

		qpair f_query;
		qkey = "mtime";
		/*
		if(totallen > 0)
		{
			int filter = 1365339058;
			int r = rand()%259200;	
			int ftime = filter - r;
			char word[12];
			sprintf(word, "%d", ftime);
			qval = word;
    			f_query.__set_qkey(qkey);
    			f_query.__set_qval(qval);	
		}
		*/
		qval = "1365063732";
		f_query.__set_qval(qval);
		std::vector<qpair> filter_query;
		filter_query.push_back(f_query);

		//query condition
		QTerm qt;
		//qt.__set_single_query(single_query);
		qt.__set_tags(tags);
		//qt.__set_dnf(dnf);
		//qt.__set_filter(filter_query);
	
		//query
		Query q;
		q.__set_terms(qt);
		q.__set_start(0);
		q.__set_length(50);
		std::vector<std::string> results;
		results.push_back("id");
		results.push_back("mtime");
		results.push_back("priority");
		q.__set_results(results);
	
		std::vector<QDocument> docs;
		client.search(docs,q);
		
		pthread_mutex_lock(&qsum_mutex);
		qsum++;
		pthread_mutex_unlock(&qsum_mutex);
		
		gettimeofday(&tpend, NULL);
		timeuse=1000000L*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;
		
	}
	
	transport->close();
  pthread_exit(NULL);

}

int main()
{
	int threadnum = 10;
	pthread_t *tid = new pthread_t[threadnum];
	
	TAGS t;
	load_tags(t);
	struct timeval tpstart;
	gettimeofday(&tpstart,NULL);
	ARGS ar;
	ar.t = t;
	ar.tpstart = tpstart;
	
	for(int i = 0; i < threadnum; i++)
	{
		pthread_create(&tid[i], NULL, work_thread, (void*)(&ar));
		usleep(10);
	}
	
	for(int i=0; i<threadnum; i++)
	{
		pthread_join(tid[i], NULL);
	}
	pthread_mutex_destroy(&qsum_mutex);
	delete tid;
	
	FILE *outfp = fopen("qsum.txt","a");
	if(outfp == NULL)
	{
		printf("can't open file:qsum.txt\n");
		exit(1);
	}
	fprintf(outfp, "%d\n", qsum);
	fclose(outfp);
	
	return 0;
}
