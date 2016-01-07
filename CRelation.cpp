#include "CRelation.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

CRelation::CRelation()
{
	
};

CRelation::~CRelation()
{
	
};

void CRelation::Initialize()
{
	boost::property_tree::ptree pt;
	boost::property_tree::ini_parser::read_ini("config.ini", pt);
	size_t indecesCount 			= pt.get<size_t>("config.indexes_count");
	size_t transitiveIndecesCount 	= pt.get<size_t>("config.transitive_indexes_count");
	for(size_t i = 0; i < indecesCount; i++)
	{
		string name = = pt.get<string>("index" + to_string(i) +".name");
		CIndex* pIndex = new CIndex(name);
		pIndex->Load();
		m_data[name] = pIndex;
	};
	for(size_t i = 0; i < transitiveIndecesCount; i++)
	{
		string name = pt.get<string>("transitive" + to_string(i) +".name");
		string transitiveOnName = pt.get<string>("transitive" + to_string(i) +".transitive_on");
		CIndex indexTransitiveOn = GetIndex(transitiveOnName);
		CIndex* pIndex = new CIndex(name, true, &indexTransitiveOn);
		pIndex->Load();
		m_data[name] = pIndex;
	};
};

void CRelation::Finalize()
{
	
};