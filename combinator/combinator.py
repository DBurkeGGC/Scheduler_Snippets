from src.section.section import Section
from src.schedule.schedule import Schedule


class Combinator:
    def __init__(self, term='0', filter_days=None, filter_tba=False, filter_start=None, filter_end=None, min_rating=0.0,
                 limit=1000):
        self.term = term
        self.limit = limit
        self.courses = []
        self.sections = []
        self.workingSchedules = []
        self.filter_days = filter_days
        self.filter_tba = filter_tba
        self.filter_start = filter_start
        self.filter_end = filter_end
        self.min_rating = min_rating

    def __repr__(self):
        return '<Combinator object>'

    def __iter__(self):
        return self.workingSchedules

    def add_course(self, course):
        self.courses.append(course)
        self.get_sections(course[0], course[1])

    def add_many(self, courses):
        for course in courses:
            self.get_sections(course[0], course[1])

    # def remove_course(self, course):
    #     self.courses.remove(course)

    def get_sections(self, subject, number):
        self.sections.append(Section.get_sections_filtered(subject, number, self.term, self.filter_tba, self.filter_days,
                                                           self.filter_start, self.filter_end, self.min_rating))

    # Requires empty array to be passed
    def get_course_combinations(self, schedule):
        if len(schedule) != len(self.sections):
            for item_a in self.sections[len(schedule)]:
                if len(self.workingSchedules) == self.limit:
                    break
                flag = True
                for item_b in schedule:
                    if len(self.workingSchedules) == self.limit:
                        break
                    if item_a.check_overlap(item_b):
                        flag = False
                        break
                if flag:
                    self.get_course_combinations([*schedule, item_a])
        else:
            self.workingSchedules.append(Schedule(schedule))

    def process_working(self):
        for schedule in self.workingSchedules:
            schedule.process_all()

    def do_all(self):
        self.get_course_combinations([])
        self.process_working()
