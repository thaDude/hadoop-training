#!/usr/bin/env python

import sys

# Hard-coded separator between key and values. This is also the default separator used by the Hadoop streaming package.
kv_sep = '\t'
counter_group = 'LTW_DATA_PROCESSING'


class SiteNameMapper:
    """Emit "(site <tab> 1>)" tuples. Emit means: print to standard output."""

    def __init__(self):
        self.corrupt_line_counter = 0
        self.counter_name = 'CORRUPT_LINES'

    def process_line(self, line):
        """Splits input line.

        Note, we need to protect against corrupt records here. We could have checked the number of fields also.
        """
        kv = line.split(kv_sep)
        try:
            print(kv_sep.join([kv[0], str(1)]))
        except IndexError:
            self.corrupt_line_counter += 1
            sys.stderr.write('reporter:counter:{0},{1},{2}\n'.format(
                counter_group, self.counter_name, self.corrupt_line_counter))


class SiteNameCountReducer:
    """Process "(site <tab> 1>)" tuples and count occurrences of each site value."""

    def __init__(self):
        self.current_site = None
        self.count = 0

    def process_line(self, line):
        site, value = line.split(kv_sep)

        if not self.current_site:
            self.current_site = site

        if self.current_site != site:
            print(kv_sep.join([self.current_site, str(self.count)]))
            self.current_site = site
            self.count = 0

        self.count += 1

    def print_last_result(self):
        if self.count == 0:
            return
        print(kv_sep.join([self.current_site, str(self.count)]))


if __name__ == '__main__':
    mr_flag = sys.argv[1]

    if mr_flag == 'map':
        processor = SiteNameMapper()
    elif mr_flag == 'reduce':
        processor = SiteNameCountReducer()
    else:
        raise Exception("Argument must be either \"map\" or \"reduce\"")

    for raw_line in sys.stdin:
        processor.process_line(raw_line.strip())

    if mr_flag == 'reduce':
        processor.print_last_result()
