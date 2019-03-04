from src.common.database import Database
import json


class Course(object):
    def __init__(self, subject, number, title, term, credit_hours):
        self.subject = subject
        self.number = number
        self.title = title
        self.term = term
        self.credit_hours = credit_hours

    def __repr__(self):
        return '<Course object for {} {} - {}>'.format(self.subject, self.number, self.title)

    def json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def get_course(cls, subject, number, term):
        return cls(*Database.find_one('courses', ['subject', 'number', 'term'], [subject, number, term]))

    @classmethod
    def get_all_courses(cls):
        return [cls(*course) for course in Database.find_all('courses')]

    @classmethod
    def get_courses_by_subject(cls, subject):
        return [cls(*course) for course in Database.find_all('courses', ['subject'], [subject])]

    @classmethod
    def get_courses_by_term(cls, term):
        return [cls(*course) for course in Database.find_all('courses', ['term'], [term])]
