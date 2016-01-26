
import sys
import hashlib
import shelve
import re

class LogMasker(object):

    MASKDBFILE = "umaskdb"
    UGI_P = 'ugi=(\S+)'

    def __init__(self, logfile):
        self.logfile = logfile
        self.ugi_pattern = re.compile(LogMasker.UGI_P)

    def mask_file(self):

        outfile = self.logfile + "_masked"
        with open(self.logfile, 'r') as inlog:
            with open(outfile, 'w') as outlog:
                db = shelve.open(LogMasker.MASKDBFILE)
                for l in inlog:
                    outlog.write(self.mask_line(l, db))
                db.close()
        return outfile

    def mask_line(self, line, db):

        m = self.ugi_pattern.search(line)
        if m is None:
            print('ugi not found')
            return line

        ugi = m.group(1)
        try:
            ugi_mask = db[ugi]
            print("found ugi", ugi, ugi_mask)
        except KeyError:
            ugi_mask = hashlib.md5(line.encode('utf-8')).hexdigest()
            db[ugi] = ugi_mask
            print("not found ugi", ugi, " new ", ugi_mask)

        print(ugi, ugi_mask)
        line = line.replace(ugi, ugi_mask)
        return line





if __name__ == "__main__":

    logfile = sys.argv[1]

    masker = LogMasker(logfile)
    masker.mask_file()

    #print(logfile)