class Schedule(object):
    def __init__(self, sections):
        self.sections = sections
        self.average_rating = None
        self.no_days = 0
        self.days = None
        self.has_weekend = 'No'

    def __iter__(self):
        return (section for section in self.sections)

    def __repr__(self):
        crn_str = ''
        for section in self.sections:
            crn_str += ' ' + section.crn
        return '<Schedule object for sections{}>'.format(crn_str)

    def set_average_rating(self):
        count = 0
        total = 0
        for section in self.sections:
            if section.rating.id:
                count += 1
                total += float(section.rating.average_rating)
        if count > 0:
            self.average_rating = str(round(total / count, 2))
        else:
            self.average_rating = 'N/A'

    def set_days(self):
        order = {'M': 0, 'T': 1, 'W': 2, 'R': 3, 'F': 4, 'S': 5, 'U': 6, 'TBA': 7}
        self.days = []
        for section in self.sections:
            for day in section.days:
                if day != 'TBA' and day not in self.days:
                    self.days.append(day)
        self.no_days = len(self.days)
        if 'S' in self.days or 'U' in self.days:
            self.has_weekend = 'Yes'
        self.days.sort(key=order.get)

    def process_all(self):
        self.set_average_rating()
        self.set_days()

    def print_pretty(self):
        return "<hr>".join(section.__str__() for section in self.sections)





