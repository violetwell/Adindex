namespace java mandela.indexlog.thrift

struct Document {
    1: map<string,binary> fields,
    2: optional i32 boost
}
       
service Index {
    bool addDoc(1:string topic, 2:Document doc)
    bool updateDoc(1:string topic, 2:Document doc)
    bool deleteDoc(1:string topic, 2:i32 docId)
}

struct InnerDocument {
    1: i64 id,
    2: binary data,
    3: i32 dataLen
}
       
service InnerIndex {
    list<InnerDocument> loadData(1:string topic, 2:i64 start, 3:i32 num)
}