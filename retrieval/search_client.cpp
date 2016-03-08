#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <map>
#include "md5.h"

#include "thriftSearch.h"
#include <config.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TCompactProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using boost::shared_ptr;
int64_t transUAPAQueryToI64(std::string strkey,int64_t i64Query)
{
	  if(strkey.size()<2)
			return 0;
			
		int64_t i64_res=0;	
		int64_t i64_tmp=0;
		const char* tmpkey=strkey.c_str();
		i64_tmp=(int64_t)tmpkey[0];
		i64_tmp=i64_tmp<<56;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmpkey[1];
		i64_tmp=i64_tmp<<48;
		i64_res|=i64_tmp;
		
		i64Query=i64Query>>16;
		i64_res|=i64Query;
		return i64_res;
	
}
int64_t transStrQueryToI64(std::string strkey,std::string strQuery)
{
		if(strkey.size()<2)
			return 0;
		
		int64_t i64_res=0;	
		int64_t i64_tmp=0;
		const char* tmpkey=strkey.c_str();
		i64_tmp=(int64_t)tmpkey[0];
		i64_tmp=i64_tmp<<56;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmpkey[1];
		i64_tmp=i64_tmp<<48;
		i64_res|=i64_tmp;
		
		std::string str_md5key = MD5(strQuery).toString();
		std::string str_tag=str_md5key.substr(0,3);
		str_tag+=str_md5key.substr(str_md5key.size()-3,3);
		const char* tmptag=str_tag.c_str();
		i64_tmp=(int64_t)tmptag[0];
		i64_tmp=i64_tmp<<40;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmptag[1];
		i64_tmp=i64_tmp<<32;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmptag[2];
		i64_tmp=i64_tmp<<24;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmptag[3];
		i64_tmp=i64_tmp<<16;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmptag[4];
		i64_tmp=i64_tmp<<8;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmptag[5];
		i64_res|=i64_tmp;
		
		return i64_res;
}

int64_t transIntQueryToI64(std::string strkey,int64_t i64Query)
{
		if(strkey.size()<2)
			return 0;
		if(i64Query>281474976710655)
			return 0;
			
		int64_t i64_res=0;	
		int64_t i64_tmp=0;
		const char* tmpkey=strkey.c_str();
		i64_tmp=(int64_t)tmpkey[0];
		i64_tmp=i64_tmp<<56;
		i64_res|=i64_tmp;
		
		i64_tmp=(int64_t)tmpkey[1];
		i64_tmp=i64_tmp<<48;
		i64_res|=i64_tmp;
		

		char* tmptest=(char*)&i64Query;
		char* tmpres=(char*)&i64_res;
		tmpres[5]=tmptest[0];
		tmpres[4]=tmptest[1];
		tmpres[3]=tmptest[2];
		tmpres[2]=tmptest[3];
        	tmpres[1]=tmptest[4];
        	tmpres[0]=tmptest[5];
		return i64_res;
}

void searchIndex(std::string ip, int port);
    
int main()
{
  std::multimap<string,int> indexAddr;
    
//线上服务器地址
//  indexAddr.insert(pair<string,int>("10.16.10.57",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.58",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.216",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.217",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.213",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.214",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.96",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.97",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.101",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.102",60114));
//  indexAddr.insert(pair<string,int>("10.16.10.57",60115));
//  indexAddr.insert(pair<string,int>("10.16.10.58",60115));
//  indexAddr.insert(pair<string,int>("10.16.10.216",60115));
//  indexAddr.insert(pair<string,int>("10.16.10.217",60115));
//  indexAddr.insert(pair<string,int>("10.16.10.213",60115));
//  indexAddr.insert(pair<string,int>("10.16.10.214",60115));

//测试机地址
//indexAddr.insert(pair<string,int>("10.10.68.194",60114));
  indexAddr.insert(pair<string,int>("10.16.10.67",60115));

  std::multimap<string,int>::iterator iter;

  for(iter = indexAddr.begin(); iter != indexAddr.end(); iter++)
  {

    searchIndex(iter->first, iter->second);
  }
}


void searchIndex(std::string ip, int port)
{
      boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
    	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    
    	transport->open();
    	thriftSearchClient client(protocol);
    
    	//single_query
    	qpair s_query1;
    	std::string qkey = "size";
    	std::string qval = "9800100";
    	s_query1.__set_qkey(qkey);
    	s_query1.__set_qval(qval);
    
    	qpair s_query2;
    	qkey = "id";
    	qval = "142531";
    	s_query2.__set_qkey(qkey);
    	s_query2.__set_qval(qval);
    
    //	qpair s_query3;
    //	qkey = "adsource";
    //	qval = "0";
    //	s_query3.__set_qkey(qkey);
    //	s_query3.__set_qval(qval);
    
    	qpair s_query3;
    	qkey = "adsource";
    	qval = "1";
    	s_query3.__set_qkey(qkey);
    	s_query3.__set_qval(qval);
    
    	std::vector<qpair> single_query;
    	//single_query.push_back(s_query1);
    	single_query.push_back(s_query2);
    	//single_query.push_back(s_query3);
    
    	//dnf query
    	std::vector<int64_t> dnf;
    	int64_t itag =5066549580922880;
    	int64_t dnfval = transUAPAQueryToI64("ua",itag);
    	//dnf.push_back(dnfval);
    	
    	
    	string stritag ="mobile";
    	 dnfval = transStrQueryToI64("cl",stritag);
    	dnf.push_back(dnfval);
    	
    	itag =12224;
    	 dnfval = transIntQueryToI64("ap",itag);
    	//dnf.push_back(dnfval);
    	
    	itag =1;
    	dnfval = transIntQueryToI64("st",itag);
    	dnf.push_back(dnfval);
    
    
    	std::vector<int64_t> tags;
    	char tagdata[] = "2817851065255384,2823560073658794,2824215031529782,2829740964987190,2838522702546908,2840839636140565,2847305984459058,2849533625909488,2850844383003319,2852183294624170,2855235806576823,2859557319295759,2860186112116980,2869927616659882,2870135660103798,2870367097667894,2872458564487337,2886483483248233,2890395101250069,2895212648808981,2907654496666184,2911733967438556,2913466309951661,2915312758441221,2924941780862832,2926330384107359,2927274094379592,2929719011132719,2936226074806165,2936992365429320,2814749767126543,2815024654730006,2815299588866838,2815849278948367,2815849279013344,2815849279277378,2816124156854655,2816124156920297,2816124157314061,2816124157379870";
    	char *t = strtok(tagdata, ",");
    	long long val;
    	sscanf(t,"%lld",&val);
    	tags.push_back(val);
    	while ((t=strtok(NULL, ",")) != NULL) {
    		sscanf(t,"%lld",&val);
    		tags.push_back(val);
    	}
    	printf("tag: ");
    	
    	for (int i = 0; i < tags.size(); i++) {
    		printf("%lld,", tags[i]);
    	}
    	printf("\n");
    
    	//filter query
        qpair f_query;
        qkey = "contenttype";
    		qval = "128";
        f_query.__set_qkey(qkey);
        f_query.__set_qval(qval);
    	printf("qval=%s, len=%d\n", qval.c_str(), sizeof(qval.c_str()));
        std::vector<qpair> filter_query;
        filter_query.push_back(f_query);
        
    //    qpair f_query;
    //    qkey = "newcms";
    //    qval="0";
    //    string newcmsqval = "2533274799193088";
    //    long ltag = atol(newcmsqval.c_str());
    //		char strtag[8];
    //			strtag[0] = (char)(ltag>>0);
    //		strtag[1] = (char)(ltag>>8);
    //		strtag[2] = (char)(ltag>>16);
    //		strtag[3] = (char)(ltag>>24);
    //		strtag[4] = (char)(ltag>>32);
    //		strtag[5] = (char)(ltag>>40);
    //		strtag[6] = (char)(ltag>>48);
    //		strtag[7] = (char)(ltag>>56);
    //		char newcms[4];
    //		memcpy(newcms, strtag +2, 4);
    //		qval.assign(newcms,4);
    //    f_query.__set_qkey(qkey);
    //    f_query.__set_qval(qval);
    //	printf("qval=%s, len=%d\n", qval.c_str(), sizeof(qval.c_str()));
    //    std::vector<qpair> filter_query;
    //    filter_query.push_back(f_query);
    
    	//query condition
    	QTerm qt;
    	//qt.__set_single_query(single_query);
    	//qt.__set_tags(tags);
    	qt.__set_dnf(dnf);
    	//qt.__set_filter(filter_query);
    
    	//query
    	Query q;
    	q.__set_terms(qt);
    	q.__set_start(0);
    	q.__set_length(8000);
    	std::vector<std::string> results;
    	//results.push_back("adcode");
    	results.push_back("id");
    	//results.push_back("dnf");
    	//results.push_back("adscore");
    	//results.push_back("mtime");
    	results.push_back("size");
    	//results.push_back("score");
    	results.push_back("monitorkey");
    	//results.push_back("adcode");
    	q.__set_results(results);
    	//std::string sorted = "mtime";
    	//q.__set_sortedby(sorted);
    
    	std::vector<QDocument> docs;

    	client.search(docs,q);
    	
    	std::string log("/opt/index/retrieval-ad-update/retrieval/clientResult/");
    	log.append(ip);
    	std::stringstream portStr; 
      portStr << port;
      log.append(":");
      log.append(portStr.str()); 
      
    	
    	FILE* fptest = fopen(log.c_str(), "w");
    	
    	printf("printf result: size = %d \n", docs.size());
    	fprintf(fptest, "printf result: size = %d \n", docs.size());
    	for (int i = 0; i < docs.size(); i++) {
    		std::map<string,string>::iterator it;
    		for (it = docs[i].fields.begin(); it != docs[i].fields.end(); it++) {
    
    				printf("%s = %s\t",(it->first).c_str(), (it->second).c_str());
    				fprintf(fptest, "%s = %s\t",(it->first).c_str(), (it->second).c_str());
    			
    
    		}
    		 printf("\n");
    		 fprintf(fptest,"\n");
    	}
    	fclose(fptest);
    	transport->close();  
    
}
