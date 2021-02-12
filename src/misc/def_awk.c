#include <stdio.h>
#include <ctype.h>
#include <string.h>

int main()
{
	char s[1024];
	while (fgets(s, sizeof(s), stdin))
	{
		char* toks[3];
		int n;
		char* p = s;

		/* get paranoid */
		s[sizeof(s) - 1] = 0;

		/* parse string to tokens */
		toks[2] = NULL;
		for (n = 0; n < 3; ++n)
		{
			toks[n] = strtok(p, " \t\n\r");
			p = NULL;
			if (!toks[n])
				break;
		}

		/* skip empty & incomplete */
		if (! toks[2])
			continue;

		/* skip unknown statements */
		if (strcmp(toks[0], "#define") != 0)
			continue;

		/* skip unknown constants */
		for (p = toks[2]; *p; ++p)
		{
			if (isdigit(*p))
				break;
		}
		if (!*p)
			continue;

		/* output correct constant */
		if (*toks[2] == '-')
			printf("\t%s = %s;\n", toks[1], toks[2]);
		else if (strncmp(toks[2], "0x", 2) == 0)
			printf("\t%s = $%s;\n", toks[1], toks[2] + 2);
		else
			printf("\t%s = byte(%s);\n", toks[1], toks[2]);
	}

	return 0;
}

