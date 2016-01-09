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
	MPI_Finalize();
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
				delete pPCT;
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
				delete pPCT;
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
		case QUERY_11:
			if(COORDINATOR)
			{
				PCT2<size_t, size_t>* pPCTMktsegment = new PCT2<size_t, size_t>(NUM_FRAGMENTS);
				PCT2<size_t, size_t>* pPCTRevenue = new PCT2<size_t, size_t>(NUM_FRAGMENTS);
				ReceivePCT2(queryNum, pPCTMktsegment);
				ReceivePCT2(queryNum, pPCTRevenue);
				unsigned long long int resGroupSum[5] = {0, 0, 0, 0, 0};
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size1 = pPCTMktsegment->GetSize(numfr);
					size_t size2 = pPCTRevenue->GetSize(numfr);
					cout << "size1: " << size1 << "; size2: " << size2 << endl;
					#pragma omp parallel num_threads(DEF_NUM_THREADS)
					{
							
						#pragma omp for schedule(dynamic, 1)
						for(size_t i = 0; i < size1; i++)
						{
							for(int j = 0; j < size2; j++)
							{
								//cout << pPCTRevenue->GetElement1(numfr, i) << endl;
								if(pPCTMktsegment->GetElement1(numfr, i) == pPCTRevenue->GetElement1(numfr, j))
								{
									#pragma omp critical
									{
										resGroupSum[pPCTMktsegment->GetElement2(numfr, i)] += pPCTRevenue->GetElement2(numfr, j);
									};
								};	
							};
						};
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
				PCT2<size_t, size_t>* pBufPCTMktsegment = new PCT2<size_t, size_t>(DEF_NUM_THREADS);
				PCT2<size_t, size_t>* pBufPCTRevenue = new PCT2<size_t, size_t>(DEF_NUM_THREADS);
				
				PCT2<size_t, size_t>* pPCTMktsegment = new PCT2<size_t, size_t>(1);
				PCT2<size_t, size_t>* pPCTRevenue = new PCT2<size_t, size_t>(1);
				
				Query11(pBufPCTMktsegment, pBufPCTRevenue);
				
				WorkerPCT2Prepare(queryNum, pBufPCTMktsegment, pPCTMktsegment);
				WorkerPCT2Prepare(queryNum, pBufPCTRevenue, pPCTRevenue);
				SendPCT2(pPCTMktsegment);
				SendPCT2(pPCTRevenue);
				
				delete pPCTMktsegment;
				delete pBufPCTRevenue;
				delete pPCTRevenue;
				delete pBufPCTMktsegment;
			};
			break;
		case QUERY_12:
			if(COORDINATOR)
			{
				PCT2<size_t, size_t>* pPCTMktsegment = new PCT2<size_t, size_t>(NUM_FRAGMENTS);
				PCT2<size_t, size_t>* pPCTRevenue = new PCT2<size_t, size_t>(NUM_FRAGMENTS);
				ReceivePCT2(queryNum, pPCTMktsegment);
				ReceivePCT2(queryNum, pPCTRevenue);
				unsigned long long int resGroupSum[5] = {0, 0, 0, 0, 0};
				for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++)
				{
					size_t size1 = pPCTMktsegment->GetSize(numfr);
					size_t size2 = pPCTRevenue->GetSize(numfr);
					cout << "size1: " << size1 << "; size2: " << size2 << endl;
					#pragma omp parallel num_threads(DEF_NUM_THREADS)
					{	
						#pragma omp for schedule(dynamic, 1)
						for(size_t i = 0; i < size1; i++)
						{
							for(int j = 0; j < size2; j++)
							{
								//cout << pPCTRevenue->GetElement1(numfr, i) << endl;
								if(pPCTMktsegment->GetElement1(numfr, i) == pPCTRevenue->GetElement1(numfr, j))
								{
									#pragma omp critical
									{
										resGroupSum[pPCTMktsegment->GetElement2(numfr, i)] += pPCTRevenue->GetElement2(numfr, j);
									};
								};	
							};
						};
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
				PCT2<size_t, size_t>* pBufPCTMktsegment = new PCT2<size_t, size_t>(DEF_NUM_THREADS);
				PCT2<size_t, size_t>* pBufPCTRevenue = new PCT2<size_t, size_t>(DEF_NUM_THREADS);
				
				PCT2<size_t, size_t>* pPCTMktsegment = new PCT2<size_t, size_t>(1);
				PCT2<size_t, size_t>* pPCTRevenue = new PCT2<size_t, size_t>(1);
				
				Query12(pBufPCTMktsegment, pBufPCTRevenue);
				
				WorkerPCT2Prepare(queryNum, pBufPCTMktsegment, pPCTMktsegment);
				WorkerPCT2Prepare(queryNum, pBufPCTRevenue, pPCTRevenue);
				SendPCT2(pPCTMktsegment);
				SendPCT2(pPCTRevenue);
				
				delete pPCTMktsegment;
				delete pBufPCTRevenue;
				delete pPCTRevenue;
				delete pBufPCTMktsegment;
			};
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
			delete[] requests;
			break;
	};
	
};

template <typename T1, typename T2 >
void CDBMS::ReceivePCT2(QueryType queryNum, PCT2<T1, T2>* pPCT)
{
	switch(queryNum)
	{
		case QUERY_9:
		{
			// vector of keys
			vector<vector<T1> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<T2> > data(NUM_FRAGMENTS);
			size_t size = 0;
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				MPI_Status status;
				size_t source = numfr + 1;
				MPI_Recv(&size, 1, MPI_INT64_T, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				keys[numfr].resize(size);
				data[numfr].resize(size);
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				MPI_Status status;
				MPI_Recv(&(keys[numfr][0]), size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Status status;
				MPI_Recv(&(data[numfr][0]), size, MPI_UNSIGNED_LONG_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < size; i++)
					pPCT->AddElement(numfr, keys[numfr][i], data[numfr][i]);
			};
			break;
		}
		case QUERY_10:
		{
			// vector of keys
			vector<vector<T1> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<T2> > data(NUM_FRAGMENTS);
			size_t size = 0;
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				MPI_Status status;
				size_t source = numfr + 1;
				MPI_Recv(&size, 1, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				keys[numfr].resize(size);
				data[numfr].resize(size);
			}
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				MPI_Status status;
				MPI_Recv(&(keys[numfr][0]), size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Status status;
				MPI_Recv(&data[numfr][0], size, MPI_UNSIGNED_LONG_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < size; i++)
					pPCT->AddElement(numfr, keys[numfr][i], data[numfr][i]);
			};
			break;
		};
		case QUERY_11:
		{
			// vector of keys
			vector<vector<T1> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<T2> > data(NUM_FRAGMENTS);
			size_t size = 0;
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				MPI_Status status;
				size_t source = numfr + 1;
				MPI_Recv(&size, 1, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				keys[numfr].resize(size);
				data[numfr].resize(size);
				for(size_t i = 0; i < size; i++)
					keys[numfr][i] = 0;
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				
				MPI_Status status;
				MPI_Recv(&(keys[numfr][0]), size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Status status;
				MPI_Recv(&data[numfr][0], size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < size; i++)
					pPCT->AddElement(numfr, keys[numfr][i], data[numfr][i]);
			};
			break;
		};
		case QUERY_12:
		{
			// vector of keys
			vector<vector<T1> > keys(NUM_FRAGMENTS);
			// vector of sums
			vector<vector<T2> > data(NUM_FRAGMENTS);
			size_t size = 0;
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				MPI_Status status;
				size_t source = numfr + 1;
				MPI_Recv(&size, 1, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				keys[numfr].resize(size);
				data[numfr].resize(size);
				for(size_t i = 0; i < size; i++)
					keys[numfr][i] = 0;
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				
				cout << "Receiving precomputational table." << endl;
				// start receiving attribute keys
				
				MPI_Status status;
				MPI_Recv(&(keys[numfr][0]), size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr=0; numfr<NUM_FRAGMENTS; numfr++) 
			{
				size_t source = numfr + 1;
				// start receiving attribute data
				MPI_Status status;
				MPI_Recv(&data[numfr][0], size, MPI_UNSIGNED_LONG, source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			};
			
			for(size_t numfr = 0; numfr < NUM_FRAGMENTS; numfr++)
			{
				for(size_t i = 0; i < size; i++)
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

template <typename T1, typename T2 >
void CDBMS::SendPCT2(PCT2<T1, T2>* pPCT)
{
	T1 *pKey = pPCT->GetRawDataPointer1(0);
	size_t size = pPCT->GetSize(0);
	MPI_Send(&size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD);
	//cout << size << endl;
	MPI_Send(pKey, size, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD);
	//cout << size << endl;
	T2 *pValue = pPCT->GetRawDataPointer2(0);
	//cout << sizeof(size_t) << endl;
	MPI_Send(pValue, size, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD);
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

template <typename T1, typename T2 >
void CDBMS::WorkerPCT2Prepare	( 	QueryType queryNum, PCT2<T1, T2>* pInPCT,
									PCT2<T1, T2>* pOutPCT
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
		case QUERY_11:
		{
			for(size_t thNum = 0; thNum < DEF_NUM_THREADS; thNum++)
			{
				size_t size = pInPCT->GetSize(thNum);
				for(int i = 0; i < size; i++)
				{
					pOutPCT->AddElement(0, pInPCT->GetElement1(thNum, i), pInPCT->GetElement2(thNum, i));
				};
					
			};
				
			break;
		};
		case QUERY_12:
		{
			for(size_t thNum = 0; thNum < DEF_NUM_THREADS; thNum++)
			{
				size_t size = pInPCT->GetSize(thNum);
				for(int i = 0; i < size; i++)
				{
					pOutPCT->AddElement(0, pInPCT->GetElement1(thNum, i), pInPCT->GetElement2(thNum, i));
				};
					
			};
				
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

void 	CDBMS::Query11(	PCT2<size_t, size_t>* pPCTMktsegment,
						PCT2<size_t, size_t>* pPCTRevenue
						)
{
	const size_t REGION = 1;
	const size_t WEIGHT = 20;
	
	CIndex* pICustomerIdCustomer 				= m_relation.GetIndex("I_CUSTOMER_ID_CUSTOMER");
	CIndex* pICustomerRegionTransIdCustomer 	= m_relation.GetIndex("I_CUSTOMER_REGION_TRANSITIVE_ID_CUSTOMER");
	CIndex* pICustomerMktsegmentTransIdCustomer = m_relation.GetIndex("I_CUSTOMER_MKTSEGMENT_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersIdCustomer 					= m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	CIndex* pIOrdersRevenueTransIdPart 			= m_relation.GetIndex("I_ORDERS_REVENUE_TRANSITIVE_ID_PART");
	CIndex* pIOrdersIdPart 						= m_relation.GetIndex("I_ORDERS_ID_PART");
	CIndex* pIPartIdPart 						= m_relation.GetIndex("I_PART_ID_PART");
	CIndex* pIPartWeightTransIdPart 			= m_relation.GetIndex("I_PART_WEIGHT_TRANSITIVE_ID_PART");
	
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
			
			while((customersIter < customersSize) && (ordersIter < ordersSize))
			{
				if ((pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) == pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter)) 
					&& (pICustomerRegionTransIdCustomer[0][indexSegment].GetValue(customersIter) == REGION)
					)
				{
					size_t ID2 	= pIOrdersIdCustomer[0][indexSegment].GetKey(ordersIter);
					size_t mktsegment 	= pICustomerMktsegmentTransIdCustomer[0][indexSegment].GetValue(customersIter);
					pPCTMktsegment->AddElement(numth, ID2, mktsegment);
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
			
			size_t partSize		= pIPartIdPart[0][indexSegment].GetSize();
			ordersSize			= pIOrdersIdPart[0][indexSegment].GetSize();
			ordersIter 			= 0;
			size_t partIter 	= 0;
			while((partIter < partSize) && (ordersIter < ordersSize))
			{
				if ((pIPartIdPart[0][indexSegment].GetValue(partIter) == pIOrdersIdPart[0][indexSegment].GetValue(ordersIter)) 
					&& (pIPartWeightTransIdPart[0][indexSegment].GetValue(partIter) == WEIGHT)
					)
				{
					size_t ID2 	= pIOrdersIdPart[0][indexSegment].GetKey(ordersIter);
					size_t revenue 	= pIOrdersRevenueTransIdPart[0][indexSegment].GetValue(ordersIter);
					pPCTRevenue->AddElement(numth, ID2, revenue);
					ordersIter++;
				}
				else
				{
					if (pIPartIdPart[0][indexSegment].GetValue(partIter) < pIOrdersIdPart[0][indexSegment].GetValue(ordersIter))
					{
						partIter++;
					}
					else
					{
						ordersIter++;
					};
				};
			
			};
		};
	};
};

void 	CDBMS::Query12(	PCT2<size_t, size_t>* pPCTMktsegment,
						PCT2<size_t, size_t>* pPCTRevenue
						)
{
	const size_t REGION = 2;
	const size_t WEIGHT = 20;
	
	CIndex* pICustomerIdCustomer 				= m_relation.GetIndex("I_CUSTOMER_ID_CUSTOMER");
	CIndex* pICustomerRegionTransIdCustomer 	= m_relation.GetIndex("I_CUSTOMER_REGION_TRANSITIVE_ID_CUSTOMER");
	CIndex* pICustomerMktsegmentTransIdCustomer = m_relation.GetIndex("I_CUSTOMER_MKTSEGMENT_TRANSITIVE_ID_CUSTOMER");
	CIndex* pIOrdersIdCustomer 					= m_relation.GetIndex("I_ORDERS_ID_CUSTOMER");
	CIndex* pIOrdersRevenueTransIdPart 			= m_relation.GetIndex("I_ORDERS_REVENUE_TRANSITIVE_ID_PART");
	CIndex* pIOrdersIdPart 						= m_relation.GetIndex("I_ORDERS_ID_PART");
	CIndex* pIPartIdPart 						= m_relation.GetIndex("I_PART_ID_PART");
	CIndex* pIPartWeightTransIdPart 			= m_relation.GetIndex("I_PART_WEIGHT_TRANSITIVE_ID_PART");
	
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
			
			while((customersIter < customersSize) && (ordersIter < ordersSize))
			{
				if ((pICustomerIdCustomer[0][indexSegment].GetValue(customersIter) == pIOrdersIdCustomer[0][indexSegment].GetValue(ordersIter)) 
					&& (pICustomerRegionTransIdCustomer[0][indexSegment].GetValue(customersIter) == REGION)
					)
				{
					size_t ID2 	= pIOrdersIdCustomer[0][indexSegment].GetKey(ordersIter);
					size_t Mktsegment 	= pICustomerMktsegmentTransIdCustomer[0][indexSegment].GetValue(customersIter);
					pPCTMktsegment->AddElement(numth, ID2, Mktsegment);
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
			
			size_t partSize		= pIPartIdPart[0][indexSegment].GetSize();
			ordersSize			= pIOrdersIdPart[0][indexSegment].GetSize();
			ordersIter 			= 0;
			size_t partIter 	= 0;
			while((partIter < partSize) && (ordersIter < ordersSize))
			{
				if ((pIPartIdPart[0][indexSegment].GetValue(partIter) == pIOrdersIdPart[0][indexSegment].GetValue(ordersIter)) 
					&& (pIPartWeightTransIdPart[0][indexSegment].GetValue(partIter) < WEIGHT)
					)
				{
					size_t ID2 	= pIOrdersIdPart[0][indexSegment].GetKey(ordersIter);
					size_t revenue 	= pIOrdersRevenueTransIdPart[0][indexSegment].GetValue(ordersIter);
					pPCTRevenue->AddElement(numth, ID2, revenue);
					ordersIter++;
				}
				else
				{
					if (pIPartIdPart[0][indexSegment].GetValue(partIter) < pIOrdersIdPart[0][indexSegment].GetValue(ordersIter))
					{
						partIter++;
					}
					else
					{
						ordersIter++;
					};
				};
			
			};
		};
	};
};