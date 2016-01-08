#include "mpi.h"
#include "CDBMS.h"
#include <iostream>
#include <omp.h>


size_t 	FRAGMENT_NUM 	= 0;
size_t 	NUM_FRAGMENTS	= 0;
int 	numNodes 		= 0;
int 	nodeNum 		= 0;
size_t 	DEF_NUM_THREADS	= 1;

#define WORKER (nodeNum != 0)
#define COORDINATOR (nodeNum == 0)

CDBMS::CDBMS(int *argc, char ***argv)
{
	m_pArgc = argc;
	m_paArgv = argv;
};

CDBMS::~CDBMS()
{
	
};

void CDBMS::Initialize()
{
	m_relation.Initialize();
	/*CIndex* pIndex = m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	cout << pIndex->GetSegmentsCount() << endl;
	size_t count = 0;
	for(int i = 0; i < pIndex->GetSize(); i++)
	{
		count += pIndex[0][i].GetSize();
		//cout << pIndex[0][i].GetSize() << endl;
	}
	cout << count << endl;*/
	MPI_Init(m_pArgc, m_paArgv);
	MPI_Comm_size(MPI_COMM_WORLD, &numNodes);
	NUM_FRAGMENTS = numNodes - 1;
	MPI_Comm_rank(MPI_COMM_WORLD, &nodeNum);
	FRAGMENT_NUM = nodeNum - 1;
	if (NUM_SEGMENTS % NUM_FRAGMENTS != 0)
	    throw string("ERROR: Number of segments not multiple of fragments!");
};

void CDBMS::Finalize()
{
	m_relation.Finalize();
	MPI_Finalize();
};

void CDBMS::Query(QueryType queryNum)
{	
	switch(queryNum)
	{
		case QUERY_7:
			if(COORDINATOR)
			{
				PCT1<unsigned long long int>* pPTC = new PCT1<unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT(queryNum, pPTC);
				unsigned long long int sum = 0;
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPTC->GetSize(numfr);
					for(int i = 0; i < size; i++)
						sum += pPTC->GetElement(numfr, i);
				};
				cout << "Result: " << sum << endl;
			};
			if(WORKER)
			{
				PCT1<unsigned long long int>* pBufPCT = Query7();
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(1);
				WorkerDataPrepare(queryNum, pBufPCT, pPCT);
				SendPCT(pPCT);
			}
			break;
		case QUERY_8:
			if(COORDINATOR)
			{
				PCT1<unsigned long long int>* pPTC = new PCT1<unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT(queryNum, pPTC);
				unsigned long long int sum = 0;
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPTC->GetSize(numfr);
					for(int i = 0; i < size; i++)
						sum += pPTC->GetElement(numfr, i);
				};
				cout << "Result: " << sum << endl;
			};
			if(WORKER)
			{
				PCT1<unsigned long long int>* pBufPCT = Query8();
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(1);
				WorkerDataPrepare(queryNum, pBufPCT, pPCT);
				SendPCT(pPCT);
			}
			break;
	};
};

void CDBMS::ReceivePCT(QueryType queryNum, PCT1<unsigned long long int>* pPTC)
{
	MPI_Request **requests = new MPI_Request*[NUM_FRAGMENTS];
	
	switch(queryNum)
	{
		case QUERY_7:
		case QUERY_8:
			// vector of sums
			vector<unsigned long long int> data(NUM_FRAGMENTS);
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				// receive attribute size
				size_t source = numfr + 1;
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute data
				MPI_Irecv(&(data[numfr]), 1, MPI_LONG_LONG_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, requests[numfr]);
			};
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				MPI_Status status;
				MPI_Wait(requests[numfr], &status);
			};
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				pPTC->AddElement(numfr, data[numfr]);
			}
			break;
	};
};

void CDBMS::SendPCT(PCT1<unsigned long long int>* pPTC)
{
	unsigned long long int value = pPTC->GetElement(0, 0);
	MPI_Send(&value, 1, MPI_LONG_LONG_INT, 0, 0, MPI_COMM_WORLD);
};

void CDBMS::WorkerDataPrepare(QueryType queryNum, PCT1<unsigned long long int>* pInPCT, PCT1<unsigned long long int>* pOutPCT)
{
	switch(queryNum)
	{
		case QUERY_7:
		case QUERY_8:
			unsigned long long int sum = 0;
			for(size_t thNum = 0; thNum < DEF_NUM_THREADS; thNum++)
			{
				size_t size = pInPCT->GetSize(thNum);
				for(int i = 0; i < size; i++)
					sum += pInPCT->GetElement(thNum, i);
			};
			pOutPCT->AddElement(0, sum);
			break;
	};
};

PCT1<unsigned long long int>* CDBMS::Query7()
{
	const int 	MIN_DISCOUNT 	= 1;
	const int 	MAX_DISCOUNT 	= 3;
	const int 	YEAR 			= 2014;
	
	PCT1<unsigned long long int>* pBufPCT = new PCT1<unsigned long long int>(DEF_NUM_THREADS);
	
	CIndex* pIOrdersRevenue 				= m_relation.GetIndex("I_ORDERS_REVENUE");
	CIndex* pIOrdersDiscountTransRevenue = m_relation.GetIndex("I_ORDERS_DISCOUNT_TRANSITIVE_REVENUE");
	CIndex* pIOrdersCommityearTransRevenue = m_relation.GetIndex("I_ORDERS_COMMITYEAR_TRANSITIVE_REVENUE");
	
	size_t numth = 0;
	
	#pragma omp parallel num_threads(DEF_NUM_THREADS) private(numth)
	{
			
		numth = omp_get_thread_num();	
		#pragma omp for schedule(dynamic, 1)
		for(size_t indexSegment = 0; indexSegment < pIOrdersRevenue->GetSize(); indexSegment++)
		{
			unsigned long long int sum = 0;
			for(size_t i = 0; i < pIOrdersDiscountTransRevenue[0][indexSegment].GetSize(); i++)
			{
				if((pIOrdersDiscountTransRevenue[0][indexSegment].GetValue(i) >= MIN_DISCOUNT)
					& (pIOrdersDiscountTransRevenue[0][indexSegment].GetValue(i) <= MAX_DISCOUNT)
					& (pIOrdersCommityearTransRevenue[0][indexSegment].GetValue(i) == YEAR)
				)
				{
					//cout << pIOrdersRevenue[0][indexSegment].GetValue(i) << endl;
					sum += pIOrdersRevenue[0][indexSegment].GetValue(i);
				};
				   
			};
			pBufPCT->AddElement(numth, sum);
		};
	};
	return pBufPCT;
};

PCT1<unsigned long long int>* CDBMS::Query8()
{
	const int 	MIN_DISCOUNT 	= 5;
	const int 	MAX_DISCOUNT 	= 10;
	const int 	YEAR 			= 1999;
	
	PCT1<unsigned long long int>* pBufPCT = new PCT1<unsigned long long int>(DEF_NUM_THREADS);
	
	CIndex* pIOrdersRevenue 				= m_relation.GetIndex("I_ORDERS_REVENUE");
	CIndex* pIOrdersDiscountTransRevenue = m_relation.GetIndex("I_ORDERS_DISCOUNT_TRANSITIVE_REVENUE");
	CIndex* pIOrdersCommityearTransRevenue = m_relation.GetIndex("I_ORDERS_COMMITYEAR_TRANSITIVE_REVENUE");
	
	size_t numth = 0;
	
	#pragma omp parallel num_threads(DEF_NUM_THREADS) private(numth)
	{
			
		numth = omp_get_thread_num();	
		#pragma omp for schedule(dynamic, 1)
		for(size_t indexSegment = 0; indexSegment < pIOrdersRevenue->GetSize(); indexSegment++)
		{
			unsigned long long int sum = 0;
			for(size_t i = 0; i < pIOrdersDiscountTransRevenue[0][indexSegment].GetSize(); i++)
			{
				if((pIOrdersDiscountTransRevenue[0][indexSegment].GetValue(i) >= MIN_DISCOUNT)
					& (pIOrdersDiscountTransRevenue[0][indexSegment].GetValue(i) <= MAX_DISCOUNT)
					& (pIOrdersCommityearTransRevenue[0][indexSegment].GetValue(i) < YEAR)
				)
				{
					//cout << pIOrdersRevenue[0][indexSegment].GetValue(i) << endl;
					sum += pIOrdersRevenue[0][indexSegment].GetValue(i);
				};
				   
			};
			pBufPCT->AddElement(numth, sum);
		};
	};
	return pBufPCT;
};
