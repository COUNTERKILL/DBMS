#include "CIndex.h"
#include <fstream>
#include <cstdlib>


extern size_t FRAGMENT_NUM;

CIndex::CIndex	(const string name, 
				 const bool isTransitive,
				 CIndex *indexTransitiveOn 
				)
{
	m_name 				= name;
	m_bTransitive 		= isTransitive;
	m_indexTransitiveOn = indexTransitiveOn;
	assert(!(isTransitive ^ (bool)indexTransitiveOn));
	m_data.reserve(NUM_SEGMENTS);
};

CIndex::~CIndex()
{
	
};

void CIndex::Load()
{
	size_t rowsCount = 0;
	ELEMENT_TYPE min = 0;
	ELEMENT_TYPE max = 0;
	char fileName[255] = {0};
	sprintf(fileName, "%s/%d/%s_%d.%s", DIR, NUM_FRAGMENTS, m_name.c_str(), FRAGMENT_NUM, EXT);
	// = DIR + string("/") + itoa(NUM_FRAGMENTS) + string("/") + m_name + string("_") + itoa(FRAGMENT_NUM) + string(EXT);
	if(access(fileName, 0) == -1)
	{
		throw (string("Файл ") + fileName + string(" не найден!"));
	}
	
	ifstream fInput;
	fInput.open(fileName, fstream::in);
	
	fInput >> rowsCount >> min >> max;
	
	// Determine values count of domain
	ELEMENT_TYPE domainSize = max - min + 1;
	
	ELEMENT_TYPE currentMinValue = min;
	ELEMENT_TYPE currentMaxValue = min;
	size_t currentRowNum = 0;
	for(size_t segIndex = 0; segIndex < NUM_SEGMENTS; segIndex++)
	{
		CSegment* pSegment = new CSegment();
		size_t segmentSize = 0;
		if(m_bTransitive)
		{
			CSegment transSegment = (*m_indexTransitiveOn)[segIndex];
			segmentSize = transSegment.GetSize();
			for(size_t i = 0; i < segmentSize; i++)
			{
				size_t key 			= 0;
				ELEMENT_TYPE value 	= 0;
				fInput >> key >> value;
				pSegment->AddElement(key, value);
			}
		}
		else
		{
			ELEMENT_TYPE segmentDomainSize = 0;
			segmentDomainSize = domainSize / NUM_SEGMENTS;
			if ((domainSize % NUM_SEGMENTS) - (segIndex-1) >= 1)	// введение поправки для segmentDomainSize
				segmentDomainSize++;								// в случае, если размер домена не делится 
															// без остатка на количество сегментов
			currentMaxValue = currentMinValue + segmentDomainSize - 1;
			pSegment->SetMinMaxValue(currentMinValue, currentMaxValue);
			// Adding elements to segment
			size_t key 			= 0;
			ELEMENT_TYPE value 	= 0;
			fInput >> key >> value;
			currentRowNum++;
			segmentSize = 0;
			while(value <=currentMaxValue)
			{
				segmentSize++;
				
				pSegment->AddElement(key, value);
			
				if(currentRowNum!=rowsCount)
					fInput >> key >> value;
				else
					break;
				currentRowNum++;
			};
			currentMinValue += segmentDomainSize;
		};
			
		m_data.push_back(pSegment);
	};
	fInput.close();
};

void CIndex::Initialize()
{
	Load();
};

void CIndex::Finalize()
{
	for(vector<CSegment*>::iterator segmentIter = m_data.begin(); segmentIter!=m_data.end(); segmentIter++)
	{
		delete segmentIter->second;
	};
};