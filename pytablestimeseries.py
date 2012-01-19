import logging
import sys
from threading import Lock
from datetime import datetime, timedelta

from decorator import decorator
from numpy import datetime64
import tables
from tables import *
from numpy import argmin, argmax
import re

import warnings
warnings.simplefilter("ignore", NaturalNameWarning)


# a.selectTimeInterval('i','f','d',datetime.datetime.utcnow() - datetime.timedelta(60), ,datetime.datetime.utcnow())

# note: using half-open intervals [begin, end)

class Datum(IsDescription):
    time      = Int64Col()
    value     = FloatCol()


class IntervalObservation(IsDescription):
    begin_time = Int64Col()
    end_time      = Int64Col()
    timestamp      = Int64Col()
    
    confidence = FloatCol()
    status = StringCol(16)
    source = StringCol(64)
    comment = StringCol(255)

    
    
class PyTablesTimeSeries(object):


    def __init__(self, filename, shouldFullyIndex=True, warn=logging.warn, shouldFlush=True, *args, **kwds):
        self._warn = warn
        self._lock = Lock()
        with self._lock:
            self._filename = filename
            self._file = openFile(filename, mode='a', NODE_CACHE_SLOTS=0)
              # see http://www.mail-archive.com/pytables-users@lists.sourceforge.net/msg01128.html
              # for explanation of NODE_CACHE_SLOTS
            self._file.flush()  # does this do anything?
            self._shouldFullyIndex = shouldFullyIndex
            self._shouldFlush = shouldFlush

    def _mainTableCreationFn(self, fieldGroup, item):
                    table = self._file.createTable(fieldGroup, str(item), Datum, "")
                    if self._shouldFullyIndex:
                        table.cols.time.createIndex(optlevel=9,kind='full')
                    else:
                        table.cols.time.createIndex(optlevel=2, kind='light')
                    return table

    def _intervalObservationTableCreationFn(self, fieldGroup, item):
                    table = self._file.createTable(fieldGroup, str(item), IntervalObservation, "")
                    if self._shouldFullyIndex:
                        table.cols.begin_time.createIndex(optlevel=9,kind='full')
                        table.cols.end_time.createIndex(optlevel=9,kind='full')
                    else:
                        table.cols.begin_time.createIndex(optlevel=2, kind='light')
                        table.cols.end_time.createIndex(optlevel=2, kind='light')
                    return table


    # note: must call within lock
    def _getOrCreateTable(self, item, field, duration, tableCreationFn):
            root = self._file.root

            if not str(duration) in root:
                try:
                    durationGroup = self._file.createGroup(root, str(duration))
                    fieldGroup = self._file.createGroup(durationGroup, str(field))
                    table = tableCreationFn(fieldGroup, item)
                finally:
                    self._file.flush()
            else:
                durationGroup = getattr(root, str(duration))

                if str(field) not in durationGroup:
                    try:
                        fieldGroup = self._file.createGroup(durationGroup, str(field))
                        table = tableCreationFn(fieldGroup, item)
                    finally:
                        self._file.flush()
                else:
                    fieldGroup = getattr(durationGroup, str(field))
                    if str(item) not in fieldGroup:
                        try:
                            table = tableCreationFn(fieldGroup, item)
                        finally:
                            self._file.flush()

                    else:
                        table = getattr(fieldGroup, str(item))
                
            return table


        
    @staticmethod
    def _wrapHelper(f, self, item, field, duration, tableCreationFn, *args, **kw):
        with self._lock:
            try:
                self._file.flush()
                kw['table'] = self._getOrCreateTable(item, field, duration, tableCreationFn)
                return f(self, item, field, duration, *args, **kw)
            except KeyboardInterrupt:
                raise
            except:
                orig_e_class, orig_e, orig_tb = sys.exc_info()
                newMessage = 'Exception in PyTimeSeries PyTables; closing and reopening' + (' (%s: %s)' % (orig_e_class.__name__, orig_e))
                self._warn(RuntimeWarning(newMessage), exc_info=True)
                
                try:
                    self._file.close()
                except KeyboardInterrupt:
                    raise
                except:
                    orig_e_class, orig_e, orig_tb = sys.exc_info()
                    newMessage = 'Exception in PyTimeSeries PyTables upon closing' + (' (%s: %s)' % (orig_e_class.__name__, orig_e))
                    self._warn(RuntimeWarning(newMessage), exc_info=True)

                # if there is another exception below we don't catch it
                self._file = openFile(self._filename, mode='a', NODE_CACHE_SLOTS=0)
                kw['table'] = self._getOrCreateTable(item, field, duration, tableCreationFn)
                return f(self, item, field, duration, *args, **kw) 
            finally:
                self._file.flush()
                
                
    # note: currently we create non-existant tables upon read as well as upon write
    @decorator
    def _wrap(f, self, item, field, duration, *args, **kw):
        if re.match('.*_observation', field):
            raise KeyError("PyTablesTimeSeries field names may not end with the reserved suffix '_observation'")
        return PyTablesTimeSeries._wrapHelper(f, self, item, field, duration, self._mainTableCreationFn, *args, **kw)

    # todo: would be cleaner to create subgroups "main" and "observation"
    @decorator
    def _wrap_observation(f, self, item, field, duration, *args, **kw):
        return PyTablesTimeSeries._wrapHelper(f, self, item, field + '_observation', duration, self._intervalObservationTableCreationFn, *args, **kw)                



    # note: not using _wrap because we may want to raise an error
    # todo make __getitem__ with key as a tuple
    def get_nodefault(self, item, field, time, duration, **kw):
            with self._lock:
                self._file.flush()
                table = self._getOrCreateTable(item, field, duration, self._mainTableCreationFn)
                results = table.readWhere("(time == %d)" % (datetime64(time).astype(int)))
                if not len(results):
                    self._file.flush()
                    raise KeyError('PyTablesTimeSeries: get_nodefault not found')
                result = results[0]
                self._file.flush()
                return {'time' : datetime64(result['time']).astype(object), 'value': result['value']}


    def get(self, item, field, time, duration, default=None):
        try:
            return self.get_nodefault(item, field, time, duration)
        except KeyError:
            return default
    
    # todo: does not coerce return dates back to datetime; must change dependencies if u change this
    @_wrap
    def selectTimeInterval(self, item, field, duration, beginTime, endTime, **kw):
            table = kw['table']
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
                row = table.row
                row['time'] = datetime64(time).astype(int)
                row['value'] = value
            
                row.append()
            finally:
                if self._shouldFlush:
                    self._file.flush()
                    #table.flush()

    @_wrap
    def put(self, item, field, duration, value, time = datetime64(datetime.utcnow()), **kw):
            table = kw['table']
            try:
                rows = list(table.where("(time == %d)" % (datetime64(time).astype(int))))
                if rows:
                    row = rows[0]
                else:
                    row = table.row

                row['time'] = datetime64(time).astype(int)
                row['value'] = value

                row.append()
            finally:
                if self._shouldFlush:
                    self._file.flush()
                    #table.flush()

    @_wrap_observation
    def append_interval_observation(self, item, field, duration, begin_time, end_time, timestamp=datetime.utcnow(), status='', source='', confidence=0.0, comment='', **kw):
            table = kw['table']
            try:

                row = table.row
                row['begin_time'] = datetime64(begin_time).astype(int)
                row['end_time'] = datetime64(end_time).astype(int)
                row['timestamp'] = datetime64(timestamp).astype(int)
                row['status'] = status
                row['source'] = source
                row['confidence'] = confidence
                row['comment'] = comment
            
                row.append()
            finally:
                if self._shouldFlush:
                    self._file.flush()
                    #table.flush()

    #def argsort(iterable):
    #    enumeration = list(enumerate([5,2,3])) 
    #    enumeration.sort(key=lambda (x,y):y)
        
    

    # todo: does not coerce return dates back to datetime; must change dependencies if u change this
    @_wrap_observation
    def overlapping_intervals(self, item, field, duration, begin_time, end_time, confidence_threshold=None, **kw):
            table = kw['table']
            if confidence_threshold is None:
                return table.readWhere("(end_time > %d) & (begin_time < %d)" % (datetime64(begin_time).astype(int), datetime64(end_time).astype(int)))
            else:
                return table.readWhere("(end_time > %d) & (begin_time < %d) & (confidence >= %f)" % (datetime64(begin_time).astype(int), datetime64(end_time).astype(int), confidence_threshold))
        
    
    #todo: delete_interval_observation

    def earliest_interval_observation_overlapping_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
            observed_rows = self.overlapping_intervals(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold)
            if not len(observed_rows):
                return None
            observed_beginTimes = [row['begin_time'] for row in observed_rows]
            result = observed_rows[argmin(observed_beginTimes)]
            return {'begin_time' : datetime64(result['begin_time']).astype(object), 'end_time' : datetime64(result['end_time']).astype(object), 'timestamp' : datetime64(result['timestamp']).astype(object), 'status' : result['status'], 'source' : result['source'], 'confidence' : result['confidence'], 'comment' : result['comment'], }


    def latest_interval_observation_overlapping_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
            observed_rows = self.overlapping_intervals(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold)
            if not len(observed_rows):
                return None
            observed_endTimes = [row['end_time'] for row in observed_rows]
            result = observed_rows[argmax(observed_endTimes)]
            return {'begin_time' : datetime64(result['begin_time']).astype(object), 'end_time' : datetime64(result['end_time']).astype(object), 'timestamp' : datetime64(result['timestamp']).astype(object), 'status' : result['status'], 'source' : result['source'], 'confidence' : result['confidence'], 'comment' : result['comment'], }
            
    def earliest_unobserved_time_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        time = begin_time
        while (time < end_time):
             next_observed = self.earliest_interval_observation_overlapping_interval(item, field, duration, time, end_time, confidence_threshold=confidence_threshold)
             if next_observed is None or next_observed['begin_time'] > time:
                 return time
             time = next_observed['end_time']
        return None
    
    def latest_unobserved_time_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        time = end_time
        while (begin_time < time):
             prev_observed = self.latest_interval_observation_overlapping_interval(item, field, duration, begin_time, time, confidence_threshold=confidence_threshold)
             if prev_observed is None or prev_observed['end_time'] < time:
                 return time
             time = prev_observed['begin_time']
        return None

    def unobserved_interval_hull_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        return (self.earliest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold), self.latest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold))

    @staticmethod
    def interval_union((begin_time1, end_time1), (begin_time2, end_time2)):
        if begin_time1 is None:
            begin_time = begin_time2
        elif begin_time2 is None:
            begin_time = begin_time1
        else:
            begin_time = min(begin_time1, begin_time2)

        if end_time1 is None:
            end_time = end_time2
        elif end_time2 is None:
            end_time = end_time1
        else:
            end_time = max(end_time1, end_time2)

        return (begin_time, end_time)

    # note: requires at least one field, or throws IndexError
    # beware: if you pass a string for argument 'fields' it will just take slices of the string as "field"s
    def unobserved_interval_hull_within_interval_over_fields(self, item, fields, duration, begin_time, end_time, confidence_threshold=None):
        interval = self.unobserved_interval_hull_within_interval(item, fields[0], duration, begin_time, end_time, confidence_threshold=confidence_threshold)
        for field in fields[1:]:
            interval = self.interval_union(interval, self.unobserved_interval_hull_within_interval(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold))
        return interval


    def lastEntryInTimeInterval(self, item, field, duration, beginTime, endTime):
        rows = self.selectTimeInterval(item, field, duration, beginTime, endTime)
        if not len(rows):
            return None
        times = [row['time'] for row in rows]
        result = rows[argmax(times)]
        return {'time' : datetime64(result['time']).astype(object), 'value' : result['value'], }


    def firstEntryInTimeInterval(self, item, field, duration, beginTime, endTime):
        rows = self.selectTimeInterval(item, field, duration, beginTime, endTime)
        if not len(rows):
            return None
        times = [row['time'] for row in rows]
        result = rows[argmin(times)]
        return {'time' : datetime64(result['time']).astype(object), 'value' : result['value'], }
        

    def flush(self):
        with self._lock:
            self._file.flush()


    def closestEntryInTime(self, item, field, duration, time, search_radius_duration=timedelta(3,0)):
        try:
            return self.get_nodefault(item, field, time, duration)
        except KeyError:
            lastBefore = self.lastEntryInTimeInterval(item, field, duration, time - search_radius_duration, time)
            firstAfter = self.firstEntryInTimeInterval(item, field, duration, time, time + search_radius_duration)
            if lastBefore is not None and firstAfter is None:
                return lastBefore

            if firstAfter is not None and lastBefore is None:
                return firstAfter

            if firstAfter is None and lastBefore is None:
                return None

            # only case left: neither are None
            if firstAfter['time'] - time < time - lastBefore['time']:
                return firstAfter
            else:
                return lastBefore 
                

# from pytablestimeseries import *; from datetime import datetime, timedelta; a=PyTablesTimeSeries('/tmp/b'); a.selectTimeInterval('i','f',timedelta(2),datetime.utcnow() - timedelta(60), datetime.utcnow())
# a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,3), datetime(2012,1,5))
# a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,5), datetime(2012,1,8), confidence=1.1)
# a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,5), datetime(2012,1,8))
# a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,7), datetime(2012,1,9))

# a.unobserved_interval_hull_within_interval_over_fields('i',['f', 'f2'],timedelta(2),datetime(2012,1,5), datetime(2012,1,10), confidence_threshold=0)
# Out[16]: (datetime.datetime(2012, 1, 8, 0, 0), datetime.datetime(2012, 1, 10, 0, 0))
# 

# from atr import *; b=BrokerIb(99, shouldConnect=False); s=BrokerSimulation(datetime(2012,1,11,18), 500, historySource=b, strategies = [a_strategy]); s.runSimulation(datetime(2012,1,11,18,2)); s.account.historyStorage.closestEntryInTime('netLiquidation', 'account', 0, datetime(2012, 1, 11, 18, 1,31))



# don't quite understand the following, but i'm hoping that if the client always uses managedPyTablesTimeSeries to get an instance,
# this will force all of the PyTablesTimeSeries calls to execute in a single thread,
# solving the horrible problems with threaded PyTables

# later: nope, it doesn't. i'm considering switching to cassandra.

from multiprocessing.managers import BaseManager
class MyManager(BaseManager):
    pass

MyManager.register('PyTablesTimeSeries', PyTablesTimeSeries)
manager = MyManager()
manager.start()

def managedPyTablesTimeSeries(*args, **kwds):
    return manager.PyTablesTimeSeries(*args, **kwds)
