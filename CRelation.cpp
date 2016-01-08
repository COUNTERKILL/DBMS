#include "CRelation.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <cstdio>

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
		char sectionParamName[255] = {0};
		sprintf(sectionParamName, "%s%d%s", "index", i, ".name");
		string name = pt.get<string>(sectionParamName);
		CIndex* pIndex = new CIndex(name);
		pIndex->Initialize();
		m_data[name] = pIndex;
	};
	for(size_t i = 0; i < transitiveIndecesCount; i++)
	{
		char sectionParamName[255] = {0};
		sprintf(sectionParamName, "%s%d%s", "transitive", i, ".name");
		string name = pt.get<string>(sectionParamName);
		sprintf(sectionParamName, "%s%d%s", "transitive", i, ".transitive_on");
		string transitiveOnName = pt.get<string>(sectionParamName);
		CIndex* pIndexTransitiveOn = GetIndex(transitiveOnName);
		CIndex* pIndex = new CIndex(name, true, pIndexTransitiveOn);
		pIndex->Initialize();
		m_data[name] = pIndex;
	};
};

void CRelation::Finalize()
{
	for(map<string, CIndex*>::iterator indexIter = m_data.begin(); indexIter!=m_data.end(); indexIter++)
	{
		indexIter->second->Finalize();
		delete indexIter->second;
	};
};