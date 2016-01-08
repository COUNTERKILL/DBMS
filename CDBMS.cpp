#include "CDBMS.h"
#include <iostream>

size_t FRAGMENT_NUM = 0;

CDBMS::CDBMS()
{
	
};

CDBMS::~CDBMS()
{
	
};

void CDBMS::Initialize()
{
	m_relation.Initialize();
	CIndex* pIndex = m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	cout << pIndex->GetSegmentsCount() << endl;
	size_t count = 0;
	for(int i = 0; i < pIndex->GetSize(); i++)
	{
		count += pIndex[0][i].GetSize();
		//cout << pIndex[0][i].GetSize() << endl;
	}
	cout << count << endl;
};

void CDBMS::Finalize()
{
	m_relation.Finalize();
};

void CDBMS::Query(int)
{

};
