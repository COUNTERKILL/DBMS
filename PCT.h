/*
	File: PCT.h
	Author: Stepan Prikazchikov
	About: Realization of precomputational table
*/

#pragma once
#include <vector>

template<typename T>
class PCT1
{
public:
	PCT1					(size_t numThreads) 			{ m_data.resize(numThreads); };
	~PCT1					() 								{};
public:
	void 	AddElement		(size_t numth, T elem) 			{ m_data[numth].push_back(elem); };
	T 		GetElement		(size_t numth, size_t index) 	{ return m_data[numth][index]; };
public:
	size_t 	GetSize			(size_t numth) 					{ return m_data[numth].size();};
private:	
	vector<vector<T> > 		m_data;
};

template<typename T1, typename T2>
class PCT2
{
public:
	PCT2					(size_t numThreads) 						{ m_data1.resize(numThreads); m_data2.resize(numThreads); };
	~PCT2					() 											{};
public:
	void 	AddElement		(size_t numth, T1 elem1, T2 elem2) 			{ m_data1[numth].push_back(elem1); m_data2[numth].push_back(elem2); };
	T1	 	GetElement1		(size_t numth, size_t index) 				{ return m_data1[numth][index]; };
	T2	 	GetElement2		(size_t numth, size_t index) 				{ return m_data2[numth][index]; };
public:
	size_t 	GetSize			(size_t numth) 								{ return m_data1[numth].size();};
private:	
	vector<vector<T1> > 	m_data1;
	vector<vector<T2> > 	m_data2;
};