ULIBL64PATH = /usr/local/lib64
UTHRIFTPATH = /usr/local/include/thrift
UBOOSTPATH = /usr/include/boost
 
USAMPLE_ROOT = $(PWD)/..

UTINYXMLPATH = $(USAMPLE_ROOT)/index-lib/tinyxml
ULEVELDBPATH = $(USAMPLE_ROOT)/index-lib/leveldb/include
UILTPATH = $(USAMPLE_ROOT)/thrift/indexlogThrift

UUTILPATH = $(USAMPLE_ROOT)/util
UINDEXPATH = $(USAMPLE_ROOT)/index
USEARCHPATH = $(USAMPLE_ROOT)/search
URAPIDXML = $(USAMPLE_ROOT)/util/rapidxml

UINCLUDET = -I$(ULIBL64PATH) \
	-I$(UTINYXMLPATH) \
	-I$(ULEVELDBPATH) \
	-I$(UUTILPATH) \
	-I$(UINDEXPATH) \
	-I$(USEARCHPATH) \
	-I$(URAPIDXML) \
	-I$(UILTPATH) \
	-I$(UTHRIFTPATH) \


									     

UCC=g++
UCC_FLAG=-O1
ICC_DEBUG=-g

ULIB_DIR = -lthrift -lpthread -lm -lcrypto -lstdc++

USRC= ${wildcard *.cpp}
UOBJ= ${patsubst %.cpp, %.o, $(USRC)}

Utilall:$(UOBJ)

$(UOBJ):%.o : %.cpp
	$(UCC) $(UCC_FLAG) -c $^ -o $@ $(UINCLUDET)

