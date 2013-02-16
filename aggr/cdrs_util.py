__author__ = 'acg'


import sys
from datetime import datetime
from collections import Counter

sep = "|"
dateformat = "%d/%m/%Y"


class CdrMapper:
    cur_daytype = 0
    cur_date = ""

    def __call__(self, key, value):
        """
        Parses a Cdr
        Returns (user,cell) , (Counter{ daytype&hour: cnt})
        """
        toks = value.split("|")
        day = toks[5]

        if day != self.cur_date:
            cur_date = day
            dt_obj = datetime.strptime(day, dateformat)
            dt_wkday = dt_obj.strftime("%w")

            if dt_wkday == 0:        # Sunday
                self.cur_daytype = 93
            elif 0 < dt_wkday < 5:   # Monday to Thursday
                self.cur_daytype = 90
            elif dt_wkday == 5:      # Friday
                self.cur_daytype = 91
            elif dt_wkday == 6:      # Saturday
                self.cur_daytype = 92

        daytype_hour = self.cur_daytype*1000 + int( toks[6][0:2] )

        yield (toks[0], toks[1]), Counter( { daytype_hour : 1} )



if __name__ == "__main__":
    mapper = CdrMapper()
    k = mapper(1,"013092038884|11017846||||10/10/2012|10:10:29|")
    for i in k:
        print str(i)