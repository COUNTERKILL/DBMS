/*
	File: CDBMS.h
	Author: Stepan Prikazchikov
	About: Realization of database system with coprocessor
*/

#pragma once

#include "CRelation.h"
#include "PCT.h"
#include "queryType.h"

class CDBMS
{
public:
										CDBMS				(int *argc, char ***argv);
										~CDBMS				();
public:
	void 								Initialize			();
	void 								Finalize			();
public:
	void								Query				(QueryType queryNum);
private:
	PCT1<unsigned long long int>* 		Query7				();
	PCT1<unsigned long long int>* 		Query8				();

private:
	void 								ReceivePCT			(QueryType queryNum, PCT1<unsigned long long int>* pPTC);
	void 								SendPCT				(PCT1<unsigned long long int>* pPTC);
	void 								WorkerDataPrepare	(QueryType queryNum, PCT1<unsigned long long int>* pInPCT, PCT1<unsigned long long int>* pOutPCT);
private:
	CRelation							m_relation;
	int*								m_pArgc;
	char***								m_paArgv;
};