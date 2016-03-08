#ifndef SEARCHHEADER_H
#define SEARCHHEADER_H

typedef struct _FIELDDOC {
	int doc;
	float score;
	int fields;                   /* priority field */
    int random;                   /* 随机数 */
} FieldDoc;

#endif
