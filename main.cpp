#include "CDBMS.h"
#include <iostream>

using namespace std;

int main()
{
	try
	{
		CDBMS *pDBMS = new CDBMS();
		pDBMS->Initialize();
	}
	catch(string err)
	{
		cout << err << endl;
	}
	catch(exception* err)
	{
		cout << err->what() << endl;
	}
	catch(...)
	{
		cout << "Other error" << endl;
	};
	
	return 0;
};