import json


class Time(object):
    def __init__(self, crn, start_time_str, end_time_str, start_time, end_time, day, location, start_date_str,
                 end_date_str, start_date, end_date, term, faculty):
        self.crn = crn
        self.start_time_str = start_time_str
        self.end_time_str = end_time_str
        self.start_time = start_time
        self.end_time = end_time
        self.day = day
        self.location = location
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str
        self.start_date = start_date
        self.end_date = end_date
        self.term = term
        self.faculty = faculty

    def __repr__(self):
        return '<Time object for CRN {} in term {} on day {} from {} to {}>'.format(self.crn, self.term, self.day,
                                                                                    self.start_time, self.end_time)

    def json(self):
        return json.dumps(self.__dict__)

    def check_overlap(self, other):
        if self.start_time == 'TBA' or other.start_time == 'TBA':
            return False
        if self.term != other.term:
            return False
        if self.crn == other.crn:
            return False
        if not (self.start_date <= other.end_date and self.end_date >= other.start_date):
            return False
        if self.day == other.day:
            return self.start_time <= other.end_time and self.end_time >= other.start_time
        else:
            return False
