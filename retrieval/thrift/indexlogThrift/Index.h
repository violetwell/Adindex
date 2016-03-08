/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef Index_H
#define Index_H

#include <TProcessor.h>
#include "indexlog_types.h"



class IndexIf {
 public:
  virtual ~IndexIf() {}
  virtual bool addDoc(const std::string& topic, const Document& doc) = 0;
  virtual bool updateDoc(const std::string& topic, const Document& doc) = 0;
  virtual bool deleteDoc(const std::string& topic, const int32_t docId) = 0;
};

class IndexIfFactory {
 public:
  typedef IndexIf Handler;

  virtual ~IndexIfFactory() {}

  virtual IndexIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) = 0;
  virtual void releaseHandler(IndexIf* /* handler */) = 0;
};

class IndexIfSingletonFactory : virtual public IndexIfFactory {
 public:
  IndexIfSingletonFactory(const boost::shared_ptr<IndexIf>& iface) : iface_(iface) {}
  virtual ~IndexIfSingletonFactory() {}

  virtual IndexIf* getHandler(const ::apache::thrift::TConnectionInfo&) {
    return iface_.get();
  }
  virtual void releaseHandler(IndexIf* /* handler */) {}

 protected:
  boost::shared_ptr<IndexIf> iface_;
};

class IndexNull : virtual public IndexIf {
 public:
  virtual ~IndexNull() {}
  bool addDoc(const std::string& /* topic */, const Document& /* doc */) {
    bool _return = false;
    return _return;
  }
  bool updateDoc(const std::string& /* topic */, const Document& /* doc */) {
    bool _return = false;
    return _return;
  }
  bool deleteDoc(const std::string& /* topic */, const int32_t /* docId */) {
    bool _return = false;
    return _return;
  }
};

typedef struct _Index_addDoc_args__isset {
  _Index_addDoc_args__isset() : topic(false), doc(false) {}
  bool topic;
  bool doc;
} _Index_addDoc_args__isset;

class Index_addDoc_args {
 public:

  Index_addDoc_args() : topic("") {
  }

  virtual ~Index_addDoc_args() throw() {}

  std::string topic;
  Document doc;

  _Index_addDoc_args__isset __isset;

  void __set_topic(const std::string& val) {
    topic = val;
  }

  void __set_doc(const Document& val) {
    doc = val;
  }

  bool operator == (const Index_addDoc_args & rhs) const
  {
    if (!(topic == rhs.topic))
      return false;
    if (!(doc == rhs.doc))
      return false;
    return true;
  }
  bool operator != (const Index_addDoc_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_addDoc_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class Index_addDoc_pargs {
 public:


  virtual ~Index_addDoc_pargs() throw() {}

  const std::string* topic;
  const Document* doc;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_addDoc_result__isset {
  _Index_addDoc_result__isset() : success(false) {}
  bool success;
} _Index_addDoc_result__isset;

class Index_addDoc_result {
 public:

  Index_addDoc_result() : success(0) {
  }

  virtual ~Index_addDoc_result() throw() {}

  bool success;

  _Index_addDoc_result__isset __isset;

  void __set_success(const bool val) {
    success = val;
  }

  bool operator == (const Index_addDoc_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Index_addDoc_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_addDoc_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_addDoc_presult__isset {
  _Index_addDoc_presult__isset() : success(false) {}
  bool success;
} _Index_addDoc_presult__isset;

class Index_addDoc_presult {
 public:


  virtual ~Index_addDoc_presult() throw() {}

  bool* success;

  _Index_addDoc_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _Index_updateDoc_args__isset {
  _Index_updateDoc_args__isset() : topic(false), doc(false) {}
  bool topic;
  bool doc;
} _Index_updateDoc_args__isset;

class Index_updateDoc_args {
 public:

  Index_updateDoc_args() : topic("") {
  }

  virtual ~Index_updateDoc_args() throw() {}

  std::string topic;
  Document doc;

  _Index_updateDoc_args__isset __isset;

  void __set_topic(const std::string& val) {
    topic = val;
  }

  void __set_doc(const Document& val) {
    doc = val;
  }

  bool operator == (const Index_updateDoc_args & rhs) const
  {
    if (!(topic == rhs.topic))
      return false;
    if (!(doc == rhs.doc))
      return false;
    return true;
  }
  bool operator != (const Index_updateDoc_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_updateDoc_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class Index_updateDoc_pargs {
 public:


  virtual ~Index_updateDoc_pargs() throw() {}

  const std::string* topic;
  const Document* doc;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_updateDoc_result__isset {
  _Index_updateDoc_result__isset() : success(false) {}
  bool success;
} _Index_updateDoc_result__isset;

class Index_updateDoc_result {
 public:

  Index_updateDoc_result() : success(0) {
  }

  virtual ~Index_updateDoc_result() throw() {}

  bool success;

  _Index_updateDoc_result__isset __isset;

  void __set_success(const bool val) {
    success = val;
  }

  bool operator == (const Index_updateDoc_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Index_updateDoc_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_updateDoc_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_updateDoc_presult__isset {
  _Index_updateDoc_presult__isset() : success(false) {}
  bool success;
} _Index_updateDoc_presult__isset;

class Index_updateDoc_presult {
 public:


  virtual ~Index_updateDoc_presult() throw() {}

  bool* success;

  _Index_updateDoc_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _Index_deleteDoc_args__isset {
  _Index_deleteDoc_args__isset() : topic(false), docId(false) {}
  bool topic;
  bool docId;
} _Index_deleteDoc_args__isset;

class Index_deleteDoc_args {
 public:

  Index_deleteDoc_args() : topic(""), docId(0) {
  }

  virtual ~Index_deleteDoc_args() throw() {}

  std::string topic;
  int32_t docId;

  _Index_deleteDoc_args__isset __isset;

  void __set_topic(const std::string& val) {
    topic = val;
  }

  void __set_docId(const int32_t val) {
    docId = val;
  }

  bool operator == (const Index_deleteDoc_args & rhs) const
  {
    if (!(topic == rhs.topic))
      return false;
    if (!(docId == rhs.docId))
      return false;
    return true;
  }
  bool operator != (const Index_deleteDoc_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_deleteDoc_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class Index_deleteDoc_pargs {
 public:


  virtual ~Index_deleteDoc_pargs() throw() {}

  const std::string* topic;
  const int32_t* docId;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_deleteDoc_result__isset {
  _Index_deleteDoc_result__isset() : success(false) {}
  bool success;
} _Index_deleteDoc_result__isset;

class Index_deleteDoc_result {
 public:

  Index_deleteDoc_result() : success(0) {
  }

  virtual ~Index_deleteDoc_result() throw() {}

  bool success;

  _Index_deleteDoc_result__isset __isset;

  void __set_success(const bool val) {
    success = val;
  }

  bool operator == (const Index_deleteDoc_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Index_deleteDoc_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index_deleteDoc_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Index_deleteDoc_presult__isset {
  _Index_deleteDoc_presult__isset() : success(false) {}
  bool success;
} _Index_deleteDoc_presult__isset;

class Index_deleteDoc_presult {
 public:


  virtual ~Index_deleteDoc_presult() throw() {}

  bool* success;

  _Index_deleteDoc_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class IndexClient : virtual public IndexIf {
 public:
  IndexClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) :
    piprot_(prot),
    poprot_(prot) {
    iprot_ = prot.get();
    oprot_ = prot.get();
  }
  IndexClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) :
    piprot_(iprot),
    poprot_(oprot) {
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  bool addDoc(const std::string& topic, const Document& doc);
  void send_addDoc(const std::string& topic, const Document& doc);
  bool recv_addDoc();
  bool updateDoc(const std::string& topic, const Document& doc);
  void send_updateDoc(const std::string& topic, const Document& doc);
  bool recv_updateDoc();
  bool deleteDoc(const std::string& topic, const int32_t docId);
  void send_deleteDoc(const std::string& topic, const int32_t docId);
  bool recv_deleteDoc();
 protected:
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class IndexProcessor : public ::apache::thrift::TProcessor {
 protected:
  boost::shared_ptr<IndexIf> iface_;
  virtual bool process_fn(apache::thrift::protocol::TProtocol* iprot, apache::thrift::protocol::TProtocol* oprot, std::string& fname, int32_t seqid, void* callContext);
 private:
  std::map<std::string, void (IndexProcessor::*)(int32_t, apache::thrift::protocol::TProtocol*, apache::thrift::protocol::TProtocol*, void*)> processMap_;
  void process_addDoc(int32_t seqid, apache::thrift::protocol::TProtocol* iprot, apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_updateDoc(int32_t seqid, apache::thrift::protocol::TProtocol* iprot, apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_deleteDoc(int32_t seqid, apache::thrift::protocol::TProtocol* iprot, apache::thrift::protocol::TProtocol* oprot, void* callContext);
 public:
  IndexProcessor(boost::shared_ptr<IndexIf> iface) :
    iface_(iface) {
    processMap_["addDoc"] = &IndexProcessor::process_addDoc;
    processMap_["updateDoc"] = &IndexProcessor::process_updateDoc;
    processMap_["deleteDoc"] = &IndexProcessor::process_deleteDoc;
  }

  virtual bool process(boost::shared_ptr<apache::thrift::protocol::TProtocol> piprot, boost::shared_ptr<apache::thrift::protocol::TProtocol> poprot, void* callContext);
  virtual ~IndexProcessor() {}
};

class IndexProcessorFactory : public ::apache::thrift::TProcessorFactory {
 public:
  IndexProcessorFactory(const ::boost::shared_ptr< IndexIfFactory >& handlerFactory) :
      handlerFactory_(handlerFactory) {}

  ::boost::shared_ptr< ::apache::thrift::TProcessor > getProcessor(const ::apache::thrift::TConnectionInfo& connInfo);

 protected:
  ::boost::shared_ptr< IndexIfFactory > handlerFactory_;
};

class IndexMultiface : virtual public IndexIf {
 public:
  IndexMultiface(std::vector<boost::shared_ptr<IndexIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~IndexMultiface() {}
 protected:
  std::vector<boost::shared_ptr<IndexIf> > ifaces_;
  IndexMultiface() {}
  void add(boost::shared_ptr<IndexIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  bool addDoc(const std::string& topic, const Document& doc) {
    size_t sz = ifaces_.size();
    for (size_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->addDoc(topic, doc);
      } else {
        ifaces_[i]->addDoc(topic, doc);
      }
    }
  }

  bool updateDoc(const std::string& topic, const Document& doc) {
    size_t sz = ifaces_.size();
    for (size_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->updateDoc(topic, doc);
      } else {
        ifaces_[i]->updateDoc(topic, doc);
      }
    }
  }

  bool deleteDoc(const std::string& topic, const int32_t docId) {
    size_t sz = ifaces_.size();
    for (size_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->deleteDoc(topic, docId);
      } else {
        ifaces_[i]->deleteDoc(topic, docId);
      }
    }
  }

};



#endif
