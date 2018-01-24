#!/usr/bin/env python

import sys

kv_sep = '\t'
counter_group = 'LTW_DATA_PROCESSING'


class SiteNameIdVisiteurMapper:
    """Emit "(site <tab> id_visitor <tab> 1>)" tuples. Emit means: print to standard output."""

    def __init__(self):
        self.corrupt_line_counter = 0
        self.counter_name = 'CORRUPT_LINES'

    def process_line(self, line):
        """Splits input line.

        Note, we need to protect against corrupt records here. We could have checked the number of fields also.
        """
        kv = line.split(kv_sep)
        try:
            print(kv_sep.join([kv[0], kv[1], str(1)]))
        except IndexError:
            self.corrupt_line_counter += 1
            sys.stderr.write('reporter:counter:{0},{1},{2}\n'.format(
                counter_group, self.counter_name, self.corrupt_line_counter))


class SiteNameIdVisiteurReducer:
    """Process "(site <tab> id_visitor <tab> 1>)" tuples and count id_visitor per site."""

    def __init__(self):
        self.current_site = None
        self.current_visitor = None
        self.count = 0

    def process_line(self, line):
        site, id_visitor, value = line.split(kv_sep)

        if not self.current_site:
            self.current_site = site

        if not self.current_visitor:
            self.current_visitor = id_visitor

        if self.current_site != site:
            print(kv_sep.join([self.current_site, str(self.count)]))
            self.current_site = site
            self.count = 0

        if self.current_visitor != id_visitor or self.count == 0:
            self.current_visitor = id_visitor
            self.count += 1

    def print_last_result(self):
        if self.count == 0:
            return
        print(kv_sep.join([self.current_site, str(self.count)]))


if __name__ == '__main__':
    mr_flag = sys.argv[1]

    if mr_flag == 'map':
        processor = SiteNameIdVisiteurMapper()
    elif mr_flag == 'reduce':
        processor = SiteNameIdVisiteurReducer()
    else:
        raise Exception("Argument must be either \"map\" or \"reduce\"")

    for raw_line in sys.stdin:
        processor.process_line(raw_line.strip())

    if mr_flag == 'reduce':
        processor.print_last_result()
