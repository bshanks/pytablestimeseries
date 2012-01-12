import sys
import warnings
from warnings import warn
from threading import Lock
from datetime import datetime

from decorator import decorator
from numpy import datetime64
from tables import *

class Datum(IsDescription):
    time      = Int64Col()
    value     = FloatCol()


class PyTablesTimeSeries(object):

    def __init__(self, filename, shouldFullyIndex=True, *args, **kwds):
        self._lock = Lock()
        with self._lock:
            self._filename = filename
            self._file = openFile(filename, mode='a')
            self._shouldFullyIndex = shouldFullyIndex # todo: misleading name


    # note: must call within lock
    def _getTable(self, item, field, duration):
            root = self._file.root

            if not str(duration) in root:
                try:
                    durationGroup = self._file.createGroup(root, str(duration))
                    fieldGroup = self._file.createGroup(durationGroup, str(field))
                    table = self._file.createTable(fieldGroup, str(item), Datum, "")
                    if self._shouldFullyIndex:
                        table.cols.time.createIndex(optlevel=9,kind='full')
                    else:
                        table.cols.time.createIndex(optlevel=2, kind='light')
                finally:
                    self._file.flush()
            else:
                durationGroup = getattr(root, str(duration))

                if str(field) not in durationGroup:
                    try:
                        fieldGroup = self._file.createGroup(durationGroup, str(field))
                        table = self._file.createTable(fieldGroup, str(item), Datum, "")
                        if self._shouldFullyIndex:
                            table.cols.time.createIndex(optlevel=9,kind='full')
                        else:
                            table.cols.time.createIndex(optlevel=2, kind='light')
                    finally:
                        self._file.flush()
                else:
                    fieldGroup = getattr(durationGroup, str(field))
                    if str(item) not in fieldGroup:
                        try:
                            table = self._file.createTable(fieldGroup, str(item), Datum, "")
                            if self._shouldFullyIndex:
                                table.cols.time.createIndex(optlevel=9,kind='full')
                            else:
                                table.cols.time.createIndex(optlevel=2, kind='light')
                        finally:
                            self._file.flush()

                    else:
                        table = getattr(fieldGroup, str(item))
                
            return table
                
    @decorator
    def _wrap(f, self, item, field, duration, *args, **kw):
        with self._lock:
            try:
                kw['table'] = self._getTable(item, field, duration)
                return f(self, item, field, duration, *args, **kw)
            except KeyboardInterrupt:
                raise
            except:
                orig_e_class, orig_e, orig_tb = sys.exc_info()
                newMessage = 'Exception in PyTimeSeries PyTables; closing and reopening' + (' (%s: %s)' % (orig_e_class.__name__, orig_e))
                warn(RuntimeWarning(newMessage))
                
                try:
                    self._file.close()
                except KeyboardInterrupt:
                    raise
                except:
                    orig_e_class, orig_e, orig_tb = sys.exc_info()
                    newMessage = 'Exception in PyTimeSeries PyTables upon closing' + (' (%s: %s)' % (orig_e_class.__name__, orig_e))
                    warn(RuntimeWarning(newMessage))

                self._file = openFile(self._filename, mode='a')
                kw['table'] = self._getTable(item, field, duration)
                return f(self, item, field, duration, *args, **kw) # if there is another exception here we don't catch it
                
                

    @_wrap
    def get(self, item, field, time, duration, **kw):
                table = kw['table']
                results = table.readWhere("(time == %d)" % (datetime64(time).astype(int)), field='value')
                if not results:
                    return KeyError('Requested row not found')
                return results[0]
                

    @_wrap
    def selectTimeSlice(self, item, field, duration, beginTime, endTime, **kw):
            table = kw['table']
            #print "(item == '%s') & (time >= %d) & (time < %d)" % (item, datetime64(beginTime).astype(int), datetime64(endTime).astype(int))
            rows = table.readWhere("(time >= %d) & (time < %d)" % (datetime64(beginTime).astype(int), datetime64(endTime).astype(int)))
            return rows

    @_wrap
    def select(self, item, field, duration, where, **kw):
            table = kw['table']
            rows = table.readWhere(where)
            return rows

    @_wrap
    def append(self, item, field, duration, value, time = datetime64(datetime.utcnow()), **kw):
            table = kw['table']
            try:
                #row = array([item, datetime64(time).astype(int), value])

                row = table.row
                row['time'] = datetime64(time).astype(int)
                row['value'] = value
                #table.append(row)
            
                row.append()
            finally:
                self._file.flush()

    @_wrap
    def put(self, item, field, duration, value, time = datetime64(datetime.utcnow()), **kw):
            table = kw['table']
            try:
                rows = list(table.where("(time == %d)" % (datetime64(time).astype(int))))
                if rows:
                    row = rows[0]
                else:
                    #row = array([item, datetime64(time).astype(int), value])
                    row = table.row

                row['time'] = datetime64(time).astype(int)
                row['value'] = value

                row.append()
            #table.append(row)
            finally:
                self._file.flush()
                # when just flushed table, seemed to get more "stale weak reference to dead node" AssertionError s


