#ifndef GSEC_PROTO_H
#define GSEC_PROTO_H

#include "../jrd/ThreadData.h"
#include "../common/UtilSvc.h"

// Output reporting utilities
void	GSEC_print_status(const ISC_STATUS*, bool exitOnError = true);
void	GSEC_error_redirect(const ISC_STATUS*, USHORT);
void	GSEC_error(USHORT);
void	GSEC_print(USHORT, const char* str = 0);
void	GSEC_print_partial(USHORT);
void	GSEC_diag(USHORT);

int		gsec(Firebird::UtilSvc*);
THREAD_ENTRY_DECLARE GSEC_main(THREAD_ENTRY_PARAM);

#endif // GSEC_PROTO_H

