#pragma once

#ifdef __SIZEOF_INT128__
typedef __int128 Int128;
typedef unsigned __int128 UInt128;
#else
#error Native support for __int128 is required
#endif
