from src.common.database import Database
import json


class Rating(object):
    def __init__(self, instructor, id='N/A', first_name='N/A', last_name='N/A', full_name='N/A', average_rating='N/A',
                 rating_count='N/A', easy_score='N/A', clarity_score='N/A', helpful_score='N/A', hot_score='N/A'):
        self.instructor = instructor
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.full_name = full_name
        self.average_rating = average_rating
        self.rating_count = rating_count
        self.easy_score = easy_score
        self.clarity_score = clarity_score
        self.helpful_score = helpful_score
        self.hot_score = hot_score

    def __repr__(self):
        return '<Rating object for {}>'.format(self.instructor)

    def json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def get_rating(cls, instructor):
        return cls(*Database.find_one('ratings', ['instructor'], [instructor]))

    @classmethod
    def get_all_ratings(cls):
        return [cls(*rating) for rating in Database.find_all('ratings')]
