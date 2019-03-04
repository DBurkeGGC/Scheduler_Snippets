from src.common.database import Database
from src.rating.rating import Rating
from src.course.course import Course
from src.time.time import Time
import itertools
import json


class Section(object):
    def __init__(self, crn, subject, number, section, term, hybrid, intl, credit_hours, instructor, faculty):
        self.crn = crn
        self.subject = subject
        self.number = number
        self.section = section
        self.term = term
        self.hybrid = hybrid
        self.intl = intl
        self.credit_hours = credit_hours
        self.instructor = instructor
        self.faculty = faculty
        self.title = None
        self.times = None
        self.days = None
        self.rating = None
        self.set_title()
        self.set_times()
        self.set_days()
        self.set_rating()

    def __repr__(self):
        return '<Section object for {} in term {}>'.format(self.crn, self.term)

    def __str__(self):
        order = {'M': 0, 'T': 1, 'W': 2, 'R': 3, 'F': 4, 'S': 5, 'U': 6, 'TBA': 7}
        self.times.sort(key=lambda t: order[t.day])
        section_string = '{} - {} - {} {} - {}<br>'.format(self.title, self.crn, self.subject,
                                                           self.number, self.section)
        date_string = 'Date Range: {} - {}<br>'.format(self.times[0].start_date_str, self.times[0].end_date_str)
        time_string = ''
        # TODO - Make this a table, maybe add tomato
        for time in self.times:
            time_string += '{} {} - {} ({})<br>'.format(time.day, time.start_time_str, time.end_time_str, time.location)
        if self.rating.id is not None:
            rating_string = "<div class='tooltip'><a href='http://www.ratemyprofessors.com/ShowRatings.jsp?tid=" \
                            "{}' target='_blank'>{} ({})</a><span class='tooltiptext'>{}<br><br>Overall Rating: {}" \
                            "<br>Total Ratings: {}<br>Easiness: {}<br>Clarity: {}<br>Helpfulness: {}<br>Hotness: {}" \
                            "</span></div><br><br>".format(self.rating.id, self.instructor.split('(')[0].strip(),
                                                           self.rating.average_rating, self.rating.full_name,
                                                           self.rating.average_rating, self.rating.rating_count,
                                                           self.rating.easy_score, self.rating.clarity_score,
                                                           self.rating.helpful_score, self.rating.hot_score)
        else:
            rating_string = '{} (N/A)<br><br>'.format(self.instructor.split('(')[0].strip())
        return section_string + date_string + rating_string + time_string

    # TODO - Fixme
    def json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def get_section(cls, crn, term):
        return cls(*Database.find_one('sections', ['crn', 'term'], [crn, term]))

    @classmethod
    def get_sections_filtered(cls, subject, number, term, f_tba=False, f_days=[], f_start=None,
                              f_end=None, f_min=None):
        query = "SELECT * " \
                "FROM sections " \
                "WHERE subject = %s " \
                "AND number = %s " \
                "AND term = %s " \
                "AND crn NOT IN " \
                "( SELECT DISTINCT times.crn " \
                "FROM times " \
                "JOIN sections ON times.crn = sections.crn " \
                "JOIN ratings ON sections.instructor = ratings.instructor " \
                "WHERE subject = %s " \
                "AND number = %s " \
                "AND sections.term = %s " \
                "AND ( day = ANY (%s) "
        data = [subject, number, term, subject, number, term, f_days]
        if f_start:
            query += "OR start_time <= %s "
            data.append(f_start)
        if f_end:
            query += "OR (end_time >= %s AND end_time <> 'TBA') "
            data.append(f_end)
        if f_min:
            query += "OR average_rating < %s "
            data.append(f_min)
        if f_tba:
            query += "OR day = 'TBA' OR start_time = 'TBA' "
        query += "));"
        return [cls(*i) for i in Database.find_by_query(query, data)]

    def set_title(self):
        self.title = Course.get_course(self.subject, self.number, self.term).title

    def set_times(self):
        self.times = []
        data = Database.find_all('times', ['crn', 'term'], [self.crn, self.term])
        for time in data:
            self.times.append(Time(*time))

    def set_days(self):
        self.days = []
        for time in self.times:
            if time.day not in self.days:
                self.days.append(time.day)

    def set_rating(self):
        self.rating = Rating.get_rating(self.instructor)

    def get_days(self):
        return [time.day for time in self.times]

    def check_overlap(self, other):
        for item in itertools.product(self.times, other.times):
            if item[0].check_overlap(item[1]):
                return True
        return False
