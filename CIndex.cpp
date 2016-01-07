#include "CIndex.h"
#include <fstream>

extern size_t FRAGMENT_NUM;

CIndex::CIndex	(const string name, 
				 const bool isTransitive = false,
				 const CIndex *indexTransitiveOn = NULL
				)
{
	m_name 				= name;
	m_bTransitive 		= isTransitive;
	m_indexTransitiveOn = indexTransitiveOn;
	assert(!(isTransitive ^^ (bool)indexTransitiveOn));
};

CIndex::~CIndex()
{
	
};

void CIndex::Load()
{
	size_t rowsCount = 0;
	ELEMENT_TYPE min = 0;
	ELEMENT_TYPE max = 0;
	
	string fileName = DIR + "/" + to_sting(NUM_FRAGMENTS) + "/" + m_name + "_" + to_sting(FRAGMENT_NUM) + EXT;
	if(access(fileName, 0) == -1)
	{
		throw Exception("Файл " + fileName + " не найден!");
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
				fInput >> key >> value;
				pSegment->AddElement(key, value);
			}
		}
		else
		{
			ELEMENT_TYPE segmentDomainSize = 0;
			segmentDomainSize = domainSize / NUM_SEGMENTS;
			if ((domainSize % NUM_SEGMENTS) - (i-1) >= 1)	// введение поправки для segmentDomainSize
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