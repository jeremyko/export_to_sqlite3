#include "common.hpp"

///////////////////////////////////////////////////////////////////////////////
string replaceSingleQuote( string const& original )
{
    std::string results;

    for ( std::string::const_iterator current = original.begin();
          current != original.end();
        ++ current ) 
    {
        if ( *current == '\'' ) 
        {
            results.push_back( '\'');
            cout << "replaced : " << original << endl;
        }

        results.push_back( *current );
    }

    return results;
}
