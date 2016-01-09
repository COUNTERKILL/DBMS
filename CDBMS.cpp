#include "mpi.h"
#include "CDBMS.h"
#include <iostream>
#include <omp.h>


size_t 	FRAGMENT_NUM 	= 0;
size_t 	NUM_FRAGMENTS	= 0;
int 	numNodes 		= 0;
int 	nodeNum 		= 0;
size_t 	DEF_NUM_THREADS	= 10;

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
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT1(queryNum, pPCT);
				unsigned long long int sum = 0;
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPCT->GetSize(numfr);
					for(int i = 0; i < size; i++)
						sum += pPCT->GetElement(numfr, i);
				};
				cout << "Result: " << sum << endl;
			};
			if(WORKER)
			{
				PCT1<unsigned long long int>* pBufPCT = Query7();
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(1);
				WorkerPCT1Prepare(queryNum, pBufPCT, pPCT);
				delete pBufPCT;
				SendPCT1(pPCT);
			}
			break;
		case QUERY_8:
			if(COORDINATOR)
			{
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT1(queryNum, pPCT);
				unsigned long long int sum = 0;
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPCT->GetSize(numfr);
					for(int i = 0; i < size; i++)
						sum += pPCT->GetElement(numfr, i);
				};
				cout << "Result: " << sum << endl;
			};
			if(WORKER)
			{
				PCT1<unsigned long long int>* pBufPCT = Query8();
				PCT1<unsigned long long int>* pPCT = new PCT1<unsigned long long int>(1);
				WorkerPCT1Prepare(queryNum, pBufPCT, pPCT);
				delete pBufPCT;
				SendPCT1(pPCT);
			}
			break;
		case QUERY_9:
			if(COORDINATOR)
			{
				PCT2<size_t, unsigned long long int>* pPCT = new PCT2<size_t, unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT2(queryNum, pPCT);
				unsigned long long int resGroupSum[5] = {0, 0, 0, 0, 0};
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPCT->GetSize(numfr);
					for(int i = 0; i < size; i++)
					{
						resGroupSum[pPCT->GetElement1(numfr, i)] += pPCT->GetElement2(numfr, i);
					};
				};
				cout << "Result:" << endl;
				for(size_t i = 0; i < 5; i++)
				{
					cout << "[" << i << "] [" <<  resGroupSum[i] << "]" << endl;
				};
			};
			if(WORKER)
			{
				PCT2<size_t, unsigned long long int>* pBufPCT = Query9();
				PCT2<size_t, unsigned long long int>* pPCT = new PCT2<size_t, unsigned long long int>(1);
				WorkerPCT2Prepare(queryNum, pBufPCT, pPCT);
				delete pBufPCT;
				SendPCT2(pPCT);
			}
			break;
		case QUERY_10:
			if(COORDINATOR)
			{
				PCT2<size_t, unsigned long long int>* pPCT = new PCT2<size_t, unsigned long long int>(NUM_FRAGMENTS);
				ReceivePCT2(queryNum, pPCT);
				unsigned long long int resGroupSum[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size = pPCT->GetSize(numfr);
					for(int i = 0; i < size; i++)
					{
						resGroupSum[pPCT->GetElement1(numfr, i)] += pPCT->GetElement2(numfr, i);
					};
				};
				cout << "Result:" << endl;
				for(size_t i = 0; i < 10; i++)
				{
					cout << "[" << i << "] [" <<  resGroupSum[i] << "]" << endl;
				};
			};
			if(WORKER)
			{
				PCT2<size_t, unsigned long long int>* pBufPCT = Query10();
				PCT2<size_t, unsigned long long int>* pPCT = new PCT2<size_t, unsigned long long int>(1);
				WorkerPCT2Prepare(queryNum, pBufPCT, pPCT);
				delete pBufPCT;
				SendPCT2(pPCT);
			}
			break;
	};
};

void CDBMS::ReceivePCT1(QueryType queryNum, PCT1<unsigned long long int>* pPCT)
{
	
	switch(queryNum)
	{
		case QUERY_7:
		case QUERY_8:
			MPI_Request **requests = new MPI_Request*[NUM_FRAGMENTS];
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
				pPCT->AddElement(numfr, data[numfr]);
			}
			break;
	};
	
};

void CDBMS::ReceivePCT2(QueryType queryNum, PCT2<size_t, unsigned long long int>* pPCT)
{
	switch(queryNum)
	{
		case QUERY_9:
		{
			MPI_Request **requests = new MPI_Request*[NUM_FRAGMENTS*2];
			// vector of keys
			vector<vector<size_t> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<unsigned long long int> > data(NUM_FRAGMENTS);
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				keys[numfr].resize(6);
				data[numfr].resize(6);
			}
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				MPI_Irecv(&(keys[numfr][0]), 5, MPI_LONG_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, requests[numfr]);
			};
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Irecv(&(data[numfr][0]), 5, MPI_LONG_LONG_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, requests[NUM_FRAGMENTS + numfr]);
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS*2; numfr++) 
			{
				MPI_Status status;
				MPI_Wait(requests[numfr], &status);
			};
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < 5; i++)
					pPCT->AddElement(numfr, keys[numfr][i], data[numfr][i]);
			};
			break;
		}
		case QUERY_10:
		{
			MPI_Request **requests = new MPI_Request*[NUM_FRAGMENTS*2];
			// vector of keys
			vector<vector<size_t> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<unsigned long long int> > data(NUM_FRAGMENTS);
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				keys[numfr].resize(12);
				data[numfr].resize(12);
			}
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				MPI_Irecv(&(keys[numfr][0]), 10, MPI_LONG_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, requests[numfr]);
			};
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Irecv(&(data[numfr][0]), 10, MPI_LONG_LONG_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, requests[NUM_FRAGMENTS + numfr]);
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS*2; numfr++) 
			{
				MPI_Status status;
				MPI_Wait(requests[numfr], &status);
			};
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < 10; i++)
					pPCT->AddElement(numfr, keys[numfr][i], data[numfr][i]);
			};
			break;
		};
	};
};

void CDBMS::SendPCT1(PCT1<unsigned long long int>* pPCT)
{
	unsigned long long int value = pPCT->GetElement(0, 0);
	MPI_Send(&value, 1, MPI_LONG_LONG_INT, 0, 0, MPI_COMM_WORLD);
};

void CDBMS::SendPCT2(PCT2<size_t, unsigned long long int>* pPCT)
{
	size_t *pKey = pPCT->GetRawDataPointer1(0);
	size_t size = pPCT->GetSize(0);
	MPI_Send(pKey, size, MPI_LONG_INT, 0, 0, MPI_COMM_WORLD);
	unsigned long long int *pValue = pPCT->GetRawDataPointer2(0);
	MPI_Send(pValue, size, MPI_LONG_LONG_INT, 0, 0, MPI_COMM_WORLD);
};

void CDBMS::WorkerPCT1Prepare(QueryType queryNum, PCT1<unsigned long long int>* pInPCT, PCT1<unsigned long long int>* pOutPCT)
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

void CDBMS::WorkerPCT2Prepare	( 	QueryType queryNum, PCT2<size_t, unsigned long long int>* pInPCT,
									PCT2<size_t, unsigned long long int>* pOutPCT
								)
{
	switch(queryNum)
	{
		case QUERY_9:
		{
			unsigned long long int resGroupSum[5] = {0, 0, 0, 0, 0};
			for(size_t thNum = 0; thNum < DEF_NUM_THREADS; thNum++)
			{
				size_t size = pInPCT->GetSize(thNum);
				for(int i = 0; i < size; i++)
				{
					resGroupSum[pInPCT->GetElement1(thNum, i)] += pInPCT->GetElement2(thNum, i);
				};
					
			};
			for(size_t i = 0; i < 5; i++)
				pOutPCT->AddElement(0, i, resGroupSum[i]);
			break;
		};
		case QUERY_10:
		{
			unsigned long long int resGroupSum[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			for(size_t thNum = 0; thNum < DEF_NUM_THREADS; thNum++)
			{
				size_t size = pInPCT->GetSize(thNum);
				for(int i = 0; i < size; i++)
				{
					resGroupSum[pInPCT->GetElement1(thNum, i)] += pInPCT->GetElement2(thNum, i);
				};
					
			};
			for(size_t i = 0; i < 10; i++)
				pOutPCT->AddElement(0, i, resGroupSum[i]);
			break;
		};
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
		for(size_t indexSegment = 0; indexSegment < NUM_SEGMENTS; indexSegment++)
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
		for(size_t indexSegment = 0; indexSegment < NUM_SEGMENTS; indexSegment++)
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


PCT2<size_t, unsigned long long int>* CDBMS::Query9()
{
	const size_t	MKTSEGMENT 			= 1;
	const size_t 	COMMITYEAR 			= 2015;
	
	PCT2<size_t, unsigned long long int>* pBufPCT 	= new PCT2<size_t, unsigned long long int>(DEF_NUM_THREADS);
	
	CIndex* pICustomerIdCustomer 				= m_relation.GetIndex("I_CUSTOMER_ID_CUSTOMER");
	CIndex* pICustomerRegionTransIdCustomer 	= m_relation.GetIndex("I_CUSTOMER_REGION_TRANSITIVE_ID_CUSTOMER");
	CIndex* pICustomerMktsegmentTransIdCustomer = m_relation.GetIndex("I_CUSTOMER_MKTSEGMENT_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersIdCustomer 					= m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	CIndex* pIOrdersRevenueTransIdCustomer 		= m_relation.GetIndex("I_ORDERS_REVENUE_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersCommityearTransIdCustomer 	= m_relation.GetIndex("I_ORDERS_COMMITYEAR_TRANSITIVE_ID_CUSTOMER");
	
	size_t numth = 0;
	
	#pragma omp parallel num_threads(DEF_NUM_THREADS) private(numth)
	{
			
		numth = omp_get_thread_num();	
		#pragma omp for schedule(dynamic, 1)
		for(size_t indexSegment = 0; indexSegment < NUM_SEGMENTS; indexSegment++)
		{
			PCT2<size_t, size_t>* joinRES	= new PCT2<size_t, size_t>(1);
			size_t customersIter 			= 0;
			size_t ordersIter 				= 0;
			size_t customersSize			= pICustomerIdCustomer[0][indexSegment].GetSize();
			size_t ordersSize				= pIOrdersIdCustomer[0][indexSegment].GetSize();
			unsigned long long int resGroupSum[5] = {0, 0, 0, 0, 0};
			while((customersIter < customersSize) && (ordersIter < ordersSize))
			{
				if ((pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) == pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter)) 
					&& (pICustomerMktsegmentTransIdCustomer[0][indexSegment].GetValue(customersIter) == MKTSEGMENT)
					&& (pIOrdersCommityearTransIdCustomer[0][indexSegment].GetValue(ordersIter) == COMMITYEAR)
					)
				{
					size_t revenue 	= pIOrdersRevenueTransIdCustomer[0][indexSegment].GetValue(ordersIter);
					size_t region 	= pICustomerRegionTransIdCustomer[0][indexSegment].GetValue(customersIter);
					resGroupSum[region] += revenue;
					ordersIter++;
				}
				else
				{
					if (pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) < pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter))
					{
						customersIter++;
					}
					else
					{
						ordersIter++;
					};
				};
			
			};
			for(size_t i = 0; i < 5; i++)
				pBufPCT->AddElement(numth, i, resGroupSum[i]);
			delete joinRES;
		};
	};
	return pBufPCT;
};

PCT2<size_t, unsigned long long int>* CDBMS::Query10()
{
	const size_t	MKTSEGMENT 			= 1;
	const size_t 	COMMITYEAR 			= 2015;
	
	PCT2<size_t, unsigned long long int>* pBufPCT 	= new PCT2<size_t, unsigned long long int>(DEF_NUM_THREADS);
	
	CIndex* pICustomerIdCustomer 				= m_relation.GetIndex("I_CUSTOMER_ID_CUSTOMER");
	CIndex* pICustomerCityTransIdCustomer 		= m_relation.GetIndex("I_CUSTOMER_CITY_TRANSITIVE_ID_CUSTOMER");
	CIndex* pICustomerMktsegmentTransIdCustomer = m_relation.GetIndex("I_CUSTOMER_MKTSEGMENT_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersIdCustomer 					= m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	CIndex* pIOrdersRevenueTransIdCustomer 		= m_relation.GetIndex("I_ORDERS_REVENUE_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersCommityearTransIdCustomer 	= m_relation.GetIndex("I_ORDERS_COMMITYEAR_TRANSITIVE_ID_CUSTOMER");
	
	size_t numth = 0;
	
	#pragma omp parallel num_threads(DEF_NUM_THREADS) private(numth)
	{
			
		numth = omp_get_thread_num();	
		#pragma omp for schedule(dynamic, 1)
		for(size_t indexSegment = 0; indexSegment < NUM_SEGMENTS; indexSegment++)
		{
			
			size_t customersIter 			= 0;
			size_t ordersIter 				= 0;
			size_t customersSize			= pICustomerIdCustomer[0][indexSegment].GetSize();
			size_t ordersSize				= pIOrdersIdCustomer[0][indexSegment].GetSize();
			unsigned long long int resGroupSum[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			while((customersIter < customersSize) && (ordersIter < ordersSize))
			{
				if ((pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) == pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter)) 
					&& (pICustomerMktsegmentTransIdCustomer[0][indexSegment].GetValue(customersIter) == MKTSEGMENT)
					&& (pIOrdersCommityearTransIdCustomer[0][indexSegment].GetValue(ordersIter) == COMMITYEAR)
					)
				{
					size_t revenue 	= pIOrdersRevenueTransIdCustomer[0][indexSegment].GetValue(ordersIter);
					size_t region 	= pICustomerCityTransIdCustomer[0][indexSegment].GetValue(customersIter);
					resGroupSum[region] += revenue;
					ordersIter++;
				}
				else
				{
					if (pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) < pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter))
					{
						customersIter++;
					}
					else
					{
						ordersIter++;
					};
				};
			
			};
			for(size_t i = 0; i < 10; i++)
				pBufPCT->AddElement(numth, i, resGroupSum[i]);
			
		};
	};
	return pBufPCT;
};