struct qpair {

	1: string qkey,
	2: string qval,

}

struct QTerm {

	1: list<qpair> single_query,
	2: list<qpair> filter,
	3: list<i64> tags,
	4: list<i64> dnf,

}

struct QDocument {

	1:map<string, string> fields,

}

struct Query {

	1: required QTerm terms,
	2: required i32 start,
	3: required i32 length,
	4: optional string sortedby,
	5: optional list<string> results,


}

service thriftSearch {
	list<QDocument> search(
		1: Query q,
	)
	list<string> getMonitorKey()
}

